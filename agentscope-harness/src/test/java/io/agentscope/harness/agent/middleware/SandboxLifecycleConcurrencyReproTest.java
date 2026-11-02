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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.harness.agent.filesystem.sandbox.SandboxBackedFilesystem;
import io.agentscope.harness.agent.sandbox.ExecResult;
import io.agentscope.harness.agent.sandbox.Sandbox;
import io.agentscope.harness.agent.sandbox.SandboxAcquireResult;
import io.agentscope.harness.agent.sandbox.SandboxClient;
import io.agentscope.harness.agent.sandbox.SandboxContext;
import io.agentscope.harness.agent.sandbox.SandboxLease;
import io.agentscope.harness.agent.sandbox.SandboxManager;
import io.agentscope.harness.agent.sandbox.SandboxState;
import io.agentscope.harness.agent.sandbox.SessionSandboxStateStore;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/**
 * Regression for issue #2490 — concurrent calls must not corrupt each other's sandbox binding.
 *
 * <p>Starlight deliberately serializes sandbox call windows within one middleware instance,
 * including different sessions, to protect context-free filesystem callers as well as invocation
 * bindings. A waiting call may acquire only after the preceding call has completed cleanup.
 *
 * <p>The isolation invariants still apply: A's execution and release affect only A; once B acquires,
 * late cleanup from A must neither unbind B nor route stale A contexts to B. All asynchronous waits
 * are bounded so a broken call-window lifecycle fails instead of hanging the test suite.
 */
class SandboxLifecycleConcurrencyReproTest {

    @Test
    void differentSessionsMustNotShareSandboxBinding() throws Exception {
        SandboxBackedFilesystem proxy = new SandboxBackedFilesystem();

        RecordingSandbox sandboxA = new RecordingSandbox("s1");
        RecordingSandbox sandboxB = new RecordingSandbox("s2");
        RecordingLease leaseA = new RecordingLease();
        RecordingLease leaseB = new RecordingLease();

        Map<String, RecordingSandbox> sandboxBySession =
                new ConcurrentHashMap<>(Map.of("s1", sandboxA, "s2", sandboxB));
        Map<String, RecordingLease> leaseBySession =
                new ConcurrentHashMap<>(Map.of("s1", leaseA, "s2", leaseB));

        SandboxManager manager = fakeManager(sandboxBySession, leaseBySession);
        SandboxLifecycleMiddleware mw = new SandboxLifecycleMiddleware(manager, proxy);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        RuntimeContext ctxA = callContext("s1");
        RuntimeContext ctxB = callContext("s2");

        try {
            SandboxLifecycleMiddleware.CallHandle handleA = mw.acquireForCall(ctxA);
            CountDownLatch bStarted = new CountDownLatch(1);
            Future<SandboxLifecycleMiddleware.CallHandle> pendingB =
                    executor.submit(
                            () -> {
                                bStarted.countDown();
                                return mw.acquireForCall(ctxB);
                            });
            assertTrue(bStarted.await(5, TimeUnit.SECONDS), "B's acquire task must start");
            assertThrows(
                    TimeoutException.class,
                    () -> pendingB.get(150, TimeUnit.MILLISECONDS),
                    "B must wait while A owns the middleware's safe call window");

            // INVARIANT 1: A keeps its own binding, including the context-free fallback.
            assertSame(sandboxA, proxy.getSandbox());
            assertEquals(
                    "s1",
                    proxy.execute(handleA.getRuntimeContext(), "whoami", null).output(),
                    "call A must execute against its own sandbox, not the waiting session's");
            assertFalse(sandboxA.stopped);
            assertFalse(leaseA.closed);
            assertFalse(sandboxB.stopped);
            assertFalse(leaseB.closed);

            mw.releaseForCall(handleA);
            SandboxLifecycleMiddleware.CallHandle handleB = pendingB.get(5, TimeUnit.SECONDS);

            // INVARIANT 2: A's release affects only A; B now owns its own live binding.
            assertTrue(sandboxA.stopped, "A's release must tear down A's own sandbox");
            assertTrue(leaseA.closed, "A's release must close A's own lease");
            assertFalse(sandboxB.stopped, "A's release must NOT tear down B's sandbox");
            assertFalse(leaseB.closed, "A's release must NOT close B's lease");
            assertSame(sandboxB, proxy.getSandbox());
            assertEquals("s2", proxy.execute(handleB.getRuntimeContext(), "whoami", null).output());

            // INVARIANT 3: delayed cleanup and stale invocation contexts cannot affect B.
            mw.releaseForCall(handleA);
            proxy.clearSandboxIfCurrent(sandboxA);
            assertNull(
                    proxy.getSandbox(handleA.getRuntimeContext()),
                    "a stale A invocation must never fall through to B's sandbox");
            assertSame(sandboxB, proxy.getSandbox());
            assertFalse(sandboxB.stopped);
            assertFalse(leaseB.closed);
            assertEquals(
                    "s2",
                    proxy.execute(handleB.getRuntimeContext(), "whoami", null).output(),
                    "call B must keep working after A's late cleanup");

            mw.releaseForCall(handleB);
            assertTrue(sandboxB.stopped, "B's release must tear down B's own sandbox");
            assertTrue(leaseB.closed, "B's release must close B's own lease");
            assertNull(proxy.getSandbox());
            assertNull(proxy.getSandbox(handleB.getRuntimeContext()));
        } finally {
            try {
                mw.close();
            } finally {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    private static RuntimeContext callContext(String sessionId) {
        SandboxContext sandboxContext = SandboxContext.builder().build();
        return RuntimeContext.builder()
                .sessionId(sessionId)
                .put(SandboxContext.class, sandboxContext)
                .build();
    }

    /**
     * A {@link SandboxManager} whose {@code acquire} hands back a distinct fake sandbox per session
     * and whose {@code release} records the teardown it is asked to perform. The client/state-store
     * dependencies are unused because every consulted method is overridden.
     */
    private static SandboxManager fakeManager(
            Map<String, RecordingSandbox> sandboxBySession,
            Map<String, RecordingLease> leaseBySession) {
        SandboxClient<?> client = mock(SandboxClient.class);
        SessionSandboxStateStore stateStore = mock(SessionSandboxStateStore.class);
        return new SandboxManager(client, stateStore, "repro-agent") {
            @Override
            public SandboxAcquireResult acquire(
                    SandboxContext sandboxContext, RuntimeContext runtimeContext) {
                String sid = runtimeContext.getSessionId();
                return SandboxAcquireResult.selfManaged(
                        sandboxBySession.get(sid), leaseBySession.get(sid));
            }

            @Override
            public void release(SandboxAcquireResult result) {
                if (result != null && result.getSandbox() instanceof RecordingSandbox rs) {
                    rs.stopped = true;
                }
            }

            @Override
            public void persistState(
                    SandboxAcquireResult result,
                    SandboxContext sandboxContext,
                    RuntimeContext runtimeContext) {
                // no-op for the reproduction
            }
        };
    }

    /** Minimal {@link Sandbox} that records teardown and echoes its session id from exec. */
    private static final class RecordingSandbox implements Sandbox {

        private final String sessionId;
        volatile boolean stopped;

        RecordingSandbox(String sessionId) {
            this.sessionId = sessionId;
        }

        @Override
        public void start() {}

        @Override
        public void stop() {
            stopped = true;
        }

        @Override
        public void close() {
            stopped = true;
        }

        @Override
        public boolean isRunning() {
            return !stopped;
        }

        @Override
        public SandboxState getState() {
            return null;
        }

        @Override
        public ExecResult exec(
                RuntimeContext runtimeContext, String command, Integer timeoutSeconds) {
            return new ExecResult(0, sessionId, "", false);
        }

        @Override
        public InputStream persistWorkspace() {
            return InputStream.nullInputStream();
        }

        @Override
        public void hydrateWorkspace(InputStream archive) {}
    }

    /** {@link SandboxLease} that records whether it was closed. */
    private static final class RecordingLease implements SandboxLease {

        volatile boolean closed;

        @Override
        public void close() {
            closed = true;
        }
    }
}
