/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.harness.agent.middleware;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.harness.agent.filesystem.sandbox.SandboxBackedFilesystem;
import io.agentscope.harness.agent.sandbox.Sandbox;
import io.agentscope.harness.agent.sandbox.SandboxAcquireResult;
import io.agentscope.harness.agent.sandbox.SandboxContext;
import io.agentscope.harness.agent.sandbox.SandboxInvocationKey;
import io.agentscope.harness.agent.sandbox.SandboxManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Middleware that manages the sandbox session lifecycle around each agent call.
 *
 * <h2>Pre-{@code next.apply}</h2>
 * <ol>
 *   <li>Read {@link SandboxContext} from the current {@link RuntimeContext}</li>
 *   <li>Acquire a session via {@link SandboxManager}</li>
 *   <li>Start the session (4-branch workspace init)</li>
 *   <li>Inject the live session into the {@link SandboxBackedFilesystem} proxy</li>
 * </ol>
 *
 * <h2>doFinally</h2>
 * <ol>
 *   <li>Persist sandbox session state via {@link SandboxManager} and
 *       {@link io.agentscope.harness.agent.sandbox.SessionSandboxStateStore}</li>
 *   <li>Release the session via {@link SandboxManager} (stop + optional shutdown)</li>
 *   <li>Clear the session reference from the filesystem proxy</li>
 * </ol>
 *
 * <p>Persistence failures remain diagnostic, but a self-managed sandbox release failure is a
 * hard lifecycle failure. The middleware keeps the acquire result and execution lease so the
 * exact resource can be retried; it never reports a successful call while cleanup is unconfirmed.
 * Sandbox-backed calls are serialized through a fair call-window permit because some legacy
 * Harness components still invoke the filesystem with a context-free {@code RuntimeContext}; the
 * permit is retained until exact resource cleanup is confirmed.
 */
public class SandboxLifecycleMiddleware implements HarnessRuntimeMiddleware, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SandboxLifecycleMiddleware.class);
    private static final int IMMEDIATE_RELEASE_ATTEMPTS = 2;
    private static final int DEFAULT_BACKGROUND_RELEASE_ATTEMPTS = 3;
    private static final long DEFAULT_RETRY_DELAY_MILLIS = 100L;
    private static final AtomicLong CALL_SEQUENCE = new AtomicLong();
    private static final AtomicInteger CLEANUP_THREAD_SEQUENCE = new AtomicInteger();

    private final SandboxManager sandboxManager;
    private final SandboxBackedFilesystem filesystemProxy;
    private final ConcurrentMap<CallHandle, CallState> activeCalls = new ConcurrentHashMap<>();
    private final Semaphore callWindow = new Semaphore(1, true);
    private final ScheduledExecutorService cleanupExecutor;
    private final int backgroundReleaseAttempts;
    private final long retryDelayMillis;
    private final boolean ownsCleanupExecutor;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Consumer<RuntimeContext> beforeStartCallback;

    public SandboxLifecycleMiddleware(
            SandboxManager sandboxManager, SandboxBackedFilesystem filesystemProxy) {
        this(
                sandboxManager,
                filesystemProxy,
                newCleanupExecutor(),
                DEFAULT_BACKGROUND_RELEASE_ATTEMPTS,
                DEFAULT_RETRY_DELAY_MILLIS,
                true);
    }

    SandboxLifecycleMiddleware(
            SandboxManager sandboxManager,
            SandboxBackedFilesystem filesystemProxy,
            ScheduledExecutorService cleanupExecutor,
            int backgroundReleaseAttempts,
            long retryDelayMillis) {
        this(
                sandboxManager,
                filesystemProxy,
                cleanupExecutor,
                backgroundReleaseAttempts,
                retryDelayMillis,
                false);
    }

    private SandboxLifecycleMiddleware(
            SandboxManager sandboxManager,
            SandboxBackedFilesystem filesystemProxy,
            ScheduledExecutorService cleanupExecutor,
            int backgroundReleaseAttempts,
            long retryDelayMillis,
            boolean ownsCleanupExecutor) {
        this.sandboxManager = Objects.requireNonNull(sandboxManager, "sandboxManager");
        this.filesystemProxy = Objects.requireNonNull(filesystemProxy, "filesystemProxy");
        this.cleanupExecutor = Objects.requireNonNull(cleanupExecutor, "cleanupExecutor");
        if (backgroundReleaseAttempts < 0) {
            throw new IllegalArgumentException("backgroundReleaseAttempts must be >= 0");
        }
        if (retryDelayMillis < 0) {
            throw new IllegalArgumentException("retryDelayMillis must be >= 0");
        }
        this.backgroundReleaseAttempts = backgroundReleaseAttempts;
        this.retryDelayMillis = retryDelayMillis;
        this.ownsCleanupExecutor = ownsCleanupExecutor;
    }

    /**
     * Registers a callback that runs after the sandbox session is acquired but before
     * {@link io.agentscope.harness.agent.sandbox.Sandbox#start()} applies workspace projection.
     * This allows callers to materialise resources on the host workspace (e.g.
     * {@code .skills-cache/}) so that projection picks them up in the same call.
     *
     * @param callback receives the per-call {@link RuntimeContext}; may be {@code null} to clear
     */
    public void setBeforeStartCallback(Consumer<RuntimeContext> callback) {
        this.beforeStartCallback = callback;
    }

    /**
     * Acquires the sandbox for the current call. Called from
     * {@code ReActAgent.beforeAgentExecution()} to ensure the sandbox is available
     * for both the {@code call()} and {@code streamEvents()} paths.
     *
     * @param sourceContext caller-supplied context; a unique invocation fork is returned in the
     *     handle
     * @return exact lifecycle handle that must be passed to {@link #releaseForCall(CallHandle)}
     */
    public CallHandle acquireForCall(RuntimeContext sourceContext) {
        if (closed.get()) {
            throw new IllegalStateException("Sandbox lifecycle middleware is closed");
        }

        // Always fork the caller's context. A caller is allowed to reuse the same RuntimeContext
        // instance for concurrent subscriptions; the fork is the unique routing identity used by
        // the sandbox filesystem and by the exact cleanup handle returned from this method.
        RuntimeContext invocationContext =
                RuntimeContext.builder(
                                sourceContext != null ? sourceContext : RuntimeContext.empty())
                        .build();
        SandboxInvocationKey invocationKey =
                new SandboxInvocationKey(CALL_SEQUENCE.incrementAndGet());
        invocationContext.put(SandboxInvocationKey.class, invocationKey);
        CallHandle handle = new CallHandle(invocationKey, sourceContext, invocationContext);
        SandboxContext sandboxContext = invocationContext.get(SandboxContext.class);
        if (sandboxContext == null) {
            return handle;
        }

        CallState state = new CallState(handle, sandboxContext);
        try {
            acquireCallWindow(state);
            activeCalls.put(handle, state);
            if (closed.get()) {
                throw new IllegalStateException(
                        "Sandbox lifecycle middleware closed while waiting for call window");
            }
            Consumer<RuntimeContext> cb = beforeStartCallback;
            if (cb != null) {
                try {
                    cb.accept(invocationContext);
                } catch (Exception e) {
                    log.warn(
                            "[sandbox-mw] beforeStartCallback failed; proceeding with sandbox"
                                    + " start: {}",
                            e.getMessage(),
                            e);
                }
            }
            SandboxAcquireResult result = sandboxManager.acquire(sandboxContext, invocationContext);
            Sandbox sandbox = result.getSandbox();
            try {
                synchronized (state) {
                    state.result = result;
                    if (closed.get()) {
                        throw new IllegalStateException(
                                "Sandbox lifecycle middleware closed during acquire");
                    }
                    sandbox.start();
                    if (closed.get()) {
                        throw new IllegalStateException(
                                "Sandbox lifecycle middleware closed during start");
                    }
                    filesystemProxy.setSandbox(invocationContext, sandbox);
                    filesystemProxy.setSandbox(sandbox);
                }
                log.debug(
                        "[sandbox-mw] Acquired sandbox {} for call {}",
                        sandbox.getState() != null ? sandbox.getState().getSessionId() : "?",
                        handle.invocationKey);
                return handle;
            } catch (Exception e) {
                filesystemProxy.clearSandbox(invocationContext, sandbox);
                filesystemProxy.clearSandbox(sandbox);
                int cleanupAttempts =
                        closed.get()
                                ? IMMEDIATE_RELEASE_ATTEMPTS + backgroundReleaseAttempts
                                : IMMEDIATE_RELEASE_ATTEMPTS;
                if (!attemptRelease(state, cleanupAttempts, "pre-call failure")) {
                    scheduleCleanupRetry(state, "pre-call failure");
                    Exception cleanupFailure = state.lastFailure;
                    if (cleanupFailure != null) {
                        e.addSuppressed(cleanupFailure);
                    }
                }
                throw e;
            }
        } catch (Exception e) {
            if (state.result == null) {
                activeCalls.remove(handle, state);
                releaseCallWindow(state);
            }
            log.error("[sandbox-mw] Failed to acquire/start sandbox", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Releases the sandbox after the current call. Called from
     * {@code ReActAgent.afterAgentExecution()} to ensure cleanup for both paths.
     *
     * @param handle exact handle returned by {@link #acquireForCall(RuntimeContext)}
     */
    public void releaseForCall(CallHandle handle) {
        if (handle == null) {
            return;
        }
        CallState state = activeCalls.get(handle);
        if (state == null) {
            return;
        }

        persistOnce(state);
        if (!attemptRelease(state, IMMEDIATE_RELEASE_ATTEMPTS, "post-call")) {
            scheduleCleanupRetry(state, "post-call");
            throw new IllegalStateException(
                    "Sandbox cleanup could not be confirmed during post-call", state.lastFailure);
        }
    }

    /**
     * Compatibility overload for direct middleware users. Concurrent calls that reused the same
     * source context are intentionally rejected as ambiguous; {@link CallHandle} is the only
     * concurrency-safe release key.
     */
    @Deprecated(since = "2.0.1")
    public void releaseForCall(RuntimeContext context) {
        List<CallHandle> matches = new ArrayList<>();
        for (CallState state : activeCalls.values()) {
            if (state.handle.invocationContext == context
                    || state.handle.sourceContext == context) {
                matches.add(state.handle);
            }
        }
        if (matches.isEmpty()) {
            return;
        }
        if (matches.size() != 1) {
            throw new IllegalStateException(
                    "RuntimeContext identifies multiple active sandbox calls; release by"
                            + " CallHandle");
        }
        releaseForCall(matches.get(0));
    }

    private void persistOnce(CallState state) {
        synchronized (state) {
            if (state.released || state.persistenceAttempted || state.result == null) {
                return;
            }
            state.persistenceAttempted = true;
            try {
                sandboxManager.persistState(
                        state.result, state.sandboxContext, state.handle.invocationContext);
            } catch (Exception e) {
                log.warn("[sandbox-mw] Failed to persist sandbox state: {}", e.getMessage(), e);
            }
        }
    }

    private boolean attemptRelease(CallState state, int attempts, String phase) {
        synchronized (state) {
            if (state.released) {
                return true;
            }
            SandboxAcquireResult result = state.result;
            if (result == null) {
                return false;
            }
            Exception failure = null;
            for (int attempt = 1; attempt <= attempts; attempt++) {
                try {
                    sandboxManager.release(result);
                    state.released = true;
                    activeCalls.remove(state.handle, state);
                    filesystemProxy.clearSandbox(
                            state.handle.invocationContext, result.getSandbox());
                    filesystemProxy.clearSandbox(result.getSandbox());
                    result.getLease().close();
                    releaseCallWindow(state);
                    state.lastFailure = null;
                    return true;
                } catch (Exception e) {
                    failure = e;
                    log.warn(
                            "[sandbox-mw] Sandbox release failed during {} (attempt {}/{}; call"
                                    + " {}): {}",
                            phase,
                            attempt,
                            attempts,
                            state.handle.invocationKey,
                            e.getMessage(),
                            e);
                }
            }
            state.lastFailure = failure;
            return false;
        }
    }

    private void scheduleCleanupRetry(CallState state, String phase) {
        if (closed.get() || state.released || backgroundReleaseAttempts == 0) {
            return;
        }
        if (!state.retryScheduled.compareAndSet(false, true)) {
            return;
        }
        int attempt = state.backgroundAttempts.incrementAndGet();
        if (attempt > backgroundReleaseAttempts) {
            state.retryScheduled.set(false);
            log.error(
                    "[sandbox-mw] Exhausted {} background cleanup retries for call {}; exact"
                            + " handle and lease remain retained until close()",
                    backgroundReleaseAttempts,
                    state.handle.invocationKey);
            return;
        }
        long delay = retryDelayMillis * (1L << Math.min(attempt - 1, 3));
        try {
            cleanupExecutor.schedule(
                    () -> {
                        boolean released;
                        try {
                            released =
                                    attemptRelease(
                                            state, 1, phase + " background retry " + attempt);
                        } finally {
                            state.retryScheduled.set(false);
                        }
                        if (!released) {
                            scheduleCleanupRetry(state, phase);
                        }
                    },
                    delay,
                    TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            state.retryScheduled.set(false);
            log.error(
                    "[sandbox-mw] Cleanup retry executor rejected call {}; exact handle and lease"
                            + " remain retained",
                    state.handle.invocationKey,
                    e);
        }
    }

    /**
     * Stop accepting calls and make a final bounded cleanup pass over every exact outstanding
     * handle before stopping the owned retry executor. Failed resources keep their leases fenced
     * and are logged; cleanup is never reported as confirmed.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (CallState state : List.copyOf(activeCalls.values())) {
            attemptRelease(
                    state,
                    IMMEDIATE_RELEASE_ATTEMPTS + backgroundReleaseAttempts,
                    "middleware close");
        }
        if (ownsCleanupExecutor) {
            cleanupExecutor.shutdownNow();
        }
        if (!activeCalls.isEmpty()) {
            log.error(
                    "[sandbox-mw] {} sandbox call(s) remain fenced after close; backing resource"
                            + " cleanup is unconfirmed",
                    activeCalls.size());
        }
    }

    int activeCallCount() {
        return activeCalls.size();
    }

    private void acquireCallWindow(CallState state) {
        try {
            while (!closed.get()) {
                if (callWindow.tryAcquire(100, TimeUnit.MILLISECONDS)) {
                    state.windowPermitHeld.set(true);
                    return;
                }
            }
            throw new IllegalStateException("Sandbox lifecycle middleware is closed");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for sandbox call window", e);
        }
    }

    private void releaseCallWindow(CallState state) {
        if (state.windowPermitHeld.compareAndSet(true, false)) {
            callWindow.release();
        }
    }

    private static ScheduledExecutorService newCleanupExecutor() {
        return Executors.newSingleThreadScheduledExecutor(
                task -> {
                    Thread thread =
                            new Thread(
                                    task,
                                    "sandbox-cleanup-retry-"
                                            + CLEANUP_THREAD_SEQUENCE.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                });
    }

    /** Exact per-subscription lifecycle handle. */
    public static final class CallHandle {

        private final SandboxInvocationKey invocationKey;
        private final RuntimeContext sourceContext;
        private final RuntimeContext invocationContext;

        private CallHandle(
                SandboxInvocationKey invocationKey,
                RuntimeContext sourceContext,
                RuntimeContext invocationContext) {
            this.invocationKey = invocationKey;
            this.sourceContext = sourceContext;
            this.invocationContext = invocationContext;
        }

        /** Context fork that must be passed through the complete delegate/tool call. */
        public RuntimeContext getRuntimeContext() {
            return invocationContext;
        }
    }

    private static final class CallState {

        private final CallHandle handle;
        private final SandboxContext sandboxContext;
        private final AtomicBoolean retryScheduled = new AtomicBoolean();
        private final AtomicBoolean windowPermitHeld = new AtomicBoolean();
        private final AtomicInteger backgroundAttempts = new AtomicInteger();
        private volatile SandboxAcquireResult result;
        private volatile Exception lastFailure;
        private boolean persistenceAttempted;
        private volatile boolean released;

        private CallState(CallHandle handle, SandboxContext sandboxContext) {
            this.handle = handle;
            this.sandboxContext = sandboxContext;
        }
    }
}
