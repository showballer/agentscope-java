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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.harness.agent.filesystem.sandbox.SandboxBackedFilesystem;
import io.agentscope.harness.agent.sandbox.Sandbox;
import io.agentscope.harness.agent.sandbox.SandboxAcquireResult;
import io.agentscope.harness.agent.sandbox.SandboxContext;
import io.agentscope.harness.agent.sandbox.SandboxLease;
import io.agentscope.harness.agent.sandbox.SandboxManager;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SandboxLifecycleMiddlewareCleanupTest {

    @Test
    void failedCleanupRetainsLeaseAndExactHandleUntilBackgroundRetrySucceeds() throws Exception {
        SandboxManager manager = mock(SandboxManager.class);
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        Sandbox sandbox = mock(Sandbox.class);
        AtomicBoolean leaseClosed = new AtomicBoolean();
        CountDownLatch leaseClosedSignal = new CountDownLatch(1);
        SandboxLease lease =
                () -> {
                    leaseClosed.set(true);
                    leaseClosedSignal.countDown();
                };
        SandboxAcquireResult result = SandboxAcquireResult.selfManaged(sandbox, lease);
        SandboxContext sandboxContext = SandboxContext.builder().build();
        RuntimeContext sourceContext =
                RuntimeContext.builder().put(SandboxContext.class, sandboxContext).build();
        ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor();

        when(manager.acquire(eq(sandboxContext), any(RuntimeContext.class))).thenReturn(result);
        doThrow(new Exception("rm failed"))
                .doThrow(new Exception("rm still failed"))
                .doNothing()
                .when(manager)
                .release(result);

        SandboxLifecycleMiddleware middleware =
                new SandboxLifecycleMiddleware(manager, filesystem, cleanupExecutor, 3, 100);
        try {
            SandboxLifecycleMiddleware.CallHandle handle = middleware.acquireForCall(sourceContext);

            assertNotSame(sourceContext, handle.getRuntimeContext());
            assertThrows(IllegalStateException.class, () -> middleware.releaseForCall(handle));
            assertFalse(leaseClosed.get(), "cleanup failure must keep the execution lease fenced");
            assertEquals(1, middleware.activeCallCount());
            assertSame(
                    sandbox,
                    filesystem.getSandbox(),
                    "context-free filesystem binding stays fenced with the failed cleanup");

            assertTrue(
                    leaseClosedSignal.await(2, TimeUnit.SECONDS),
                    "bounded background cleanup should run without a new business call");
            assertTrue(leaseClosed.get(), "confirmed cleanup releases the execution lease");
            assertEquals(0, middleware.activeCallCount());
            assertNull(filesystem.getSandbox(handle.getRuntimeContext()));
            assertNull(filesystem.getSandbox());
            verify(manager, times(3)).release(result);
        } finally {
            middleware.close();
            cleanupExecutor.shutdownNow();
        }
    }

    @Test
    void overlappingCallsUsingSameSourceContextSerializeWithoutOverwrite() throws Exception {
        SandboxManager manager = mock(SandboxManager.class);
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        Sandbox sandboxA = mock(Sandbox.class);
        Sandbox sandboxB = mock(Sandbox.class);
        AtomicBoolean leaseAClosed = new AtomicBoolean();
        AtomicBoolean leaseBClosed = new AtomicBoolean();
        SandboxAcquireResult resultA =
                SandboxAcquireResult.selfManaged(sandboxA, () -> leaseAClosed.set(true));
        SandboxAcquireResult resultB =
                SandboxAcquireResult.selfManaged(sandboxB, () -> leaseBClosed.set(true));
        SandboxContext sandboxContext = SandboxContext.builder().build();
        RuntimeContext sharedSourceContext =
                RuntimeContext.builder().put(SandboxContext.class, sandboxContext).build();
        CountDownLatch secondCallStarted = new CountDownLatch(1);
        CountDownLatch secondManagerAcquireEntered = new CountDownLatch(1);
        AtomicInteger acquireOrder = new AtomicInteger();
        when(manager.acquire(eq(sandboxContext), any(RuntimeContext.class)))
                .thenAnswer(
                        invocation -> {
                            int index = acquireOrder.getAndIncrement();
                            if (index == 1) {
                                secondManagerAcquireEntered.countDown();
                            }
                            return index == 0 ? resultA : resultB;
                        });

        ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
        ExecutorService callers = Executors.newFixedThreadPool(2);
        SandboxLifecycleMiddleware middleware =
                new SandboxLifecycleMiddleware(manager, filesystem, cleanupExecutor, 0, 0);
        try {
            Future<SandboxLifecycleMiddleware.CallHandle> first =
                    callers.submit(() -> middleware.acquireForCall(sharedSourceContext));
            SandboxLifecycleMiddleware.CallHandle firstHandle = first.get(2, TimeUnit.SECONDS);
            Future<SandboxLifecycleMiddleware.CallHandle> second =
                    callers.submit(
                            () -> {
                                secondCallStarted.countDown();
                                return middleware.acquireForCall(sharedSourceContext);
                            });
            assertTrue(secondCallStarted.await(2, TimeUnit.SECONDS));
            assertFalse(
                    secondManagerAcquireEntered.await(200, TimeUnit.MILLISECONDS),
                    "the fair call window must keep B from acquiring/overwriting A");

            Sandbox firstSandbox = filesystem.getSandbox(firstHandle.getRuntimeContext());
            assertSame(firstSandbox, filesystem.getSandbox());
            verify(sandboxB, never()).start();
            assertEquals(1, middleware.activeCallCount());

            middleware.releaseForCall(firstHandle);
            SandboxLifecycleMiddleware.CallHandle secondHandle = second.get(2, TimeUnit.SECONDS);

            assertNull(filesystem.getSandbox(firstHandle.getRuntimeContext()));
            assertNotSame(firstHandle.getRuntimeContext(), secondHandle.getRuntimeContext());
            Sandbox secondSandbox = filesystem.getSandbox(secondHandle.getRuntimeContext());
            assertNotSame(firstSandbox, secondSandbox);
            assertSame(secondSandbox, filesystem.getSandbox(secondHandle.getRuntimeContext()));
            assertSame(secondSandbox, filesystem.getSandbox());
            assertEquals(1, middleware.activeCallCount());
            if (firstSandbox == sandboxA) {
                assertTrue(leaseAClosed.get());
                assertFalse(leaseBClosed.get());
                verify(manager).release(resultA);
                verify(manager, times(0)).release(resultB);
            } else {
                assertTrue(leaseBClosed.get());
                assertFalse(leaseAClosed.get());
                verify(manager).release(resultB);
                verify(manager, times(0)).release(resultA);
            }

            middleware.releaseForCall(secondHandle);
            assertEquals(0, middleware.activeCallCount());
            assertNull(filesystem.getSandbox());
            assertTrue(leaseAClosed.get());
            assertTrue(leaseBClosed.get());
        } finally {
            middleware.close();
            callers.shutdownNow();
            cleanupExecutor.shutdownNow();
        }
    }

    @Test
    void closeMakesFinalBoundedAttemptForRetainedExactHandle() throws Exception {
        SandboxManager manager = mock(SandboxManager.class);
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        Sandbox sandbox = mock(Sandbox.class);
        AtomicBoolean leaseClosed = new AtomicBoolean();
        SandboxAcquireResult result =
                SandboxAcquireResult.selfManaged(sandbox, () -> leaseClosed.set(true));
        SandboxContext sandboxContext = SandboxContext.builder().build();
        RuntimeContext sourceContext =
                RuntimeContext.builder().put(SandboxContext.class, sandboxContext).build();
        when(manager.acquire(eq(sandboxContext), any(RuntimeContext.class))).thenReturn(result);
        doThrow(new Exception("first rm failure"))
                .doThrow(new Exception("second rm failure"))
                .doNothing()
                .when(manager)
                .release(result);

        ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
        SandboxLifecycleMiddleware middleware =
                new SandboxLifecycleMiddleware(manager, filesystem, cleanupExecutor, 0, 0);
        try {
            SandboxLifecycleMiddleware.CallHandle handle = middleware.acquireForCall(sourceContext);
            assertThrows(IllegalStateException.class, () -> middleware.releaseForCall(handle));
            assertFalse(leaseClosed.get());

            middleware.close();

            assertTrue(leaseClosed.get());
            assertEquals(0, middleware.activeCallCount());
            assertNull(filesystem.getSandbox(handle.getRuntimeContext()));
            verify(manager, times(3)).release(result);
        } finally {
            middleware.close();
            cleanupExecutor.shutdownNow();
        }
    }

    @Test
    void closeRacingWithAcquireCleansReturnedSandboxBeforeStart() throws Exception {
        SandboxManager manager = mock(SandboxManager.class);
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        Sandbox sandbox = mock(Sandbox.class);
        AtomicBoolean leaseClosed = new AtomicBoolean();
        SandboxAcquireResult result =
                SandboxAcquireResult.selfManaged(sandbox, () -> leaseClosed.set(true));
        SandboxContext sandboxContext = SandboxContext.builder().build();
        RuntimeContext sourceContext =
                RuntimeContext.builder().put(SandboxContext.class, sandboxContext).build();
        CountDownLatch acquireEntered = new CountDownLatch(1);
        CountDownLatch allowAcquireToReturn = new CountDownLatch(1);
        when(manager.acquire(eq(sandboxContext), any(RuntimeContext.class)))
                .thenAnswer(
                        invocation -> {
                            acquireEntered.countDown();
                            assertTrue(allowAcquireToReturn.await(2, TimeUnit.SECONDS));
                            return result;
                        });

        ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        SandboxLifecycleMiddleware middleware =
                new SandboxLifecycleMiddleware(manager, filesystem, cleanupExecutor, 0, 0);
        try {
            Future<SandboxLifecycleMiddleware.CallHandle> future =
                    caller.submit(() -> middleware.acquireForCall(sourceContext));
            assertTrue(acquireEntered.await(2, TimeUnit.SECONDS));

            middleware.close();
            allowAcquireToReturn.countDown();

            assertThrows(
                    java.util.concurrent.ExecutionException.class,
                    () -> future.get(2, TimeUnit.SECONDS));
            assertTrue(leaseClosed.get());
            assertEquals(0, middleware.activeCallCount());
            verify(sandbox, never()).start();
            verify(manager).release(result);
        } finally {
            allowAcquireToReturn.countDown();
            middleware.close();
            caller.shutdownNow();
            cleanupExecutor.shutdownNow();
        }
    }
}
