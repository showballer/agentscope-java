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
package io.agentscope.core.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;

class ExceptionUtilsTest {

    @Test
    void containsInterruptedExceptionRecognizesDirectAndWrappedInterruption() {
        InterruptedException interrupted = new InterruptedException("cancelled");

        assertTrue(ExceptionUtils.containsInterruptedException(interrupted));
        assertTrue(
                ExceptionUtils.containsInterruptedException(
                        new IllegalStateException(new RuntimeException(interrupted))));
    }

    @Test
    void containsInterruptedExceptionRejectsNullAndOrdinaryFailure() {
        assertFalse(ExceptionUtils.containsInterruptedException(null));
        assertFalse(
                ExceptionUtils.containsInterruptedException(
                        new IllegalStateException("ordinary failure")));
    }

    @Test
    void containsInterruptedExceptionTerminatesOnCircularCauseChain() {
        IllegalStateException first = new IllegalStateException("first");
        IllegalStateException second = new IllegalStateException("second", first);
        first.initCause(second);

        assertFalse(ExceptionUtils.containsInterruptedException(first));
        assertFalse(ExceptionUtils.containsCancellationException(first));
    }

    @Test
    void containsCancellationExceptionRecognizesDirectAndWrappedCancellation() {
        CancellationException cancelled = new CancellationException("cancelled");

        assertTrue(ExceptionUtils.containsCancellationException(cancelled));
        assertTrue(
                ExceptionUtils.containsCancellationException(new IllegalStateException(cancelled)));
        assertFalse(ExceptionUtils.containsCancellationException(null));
        assertFalse(
                ExceptionUtils.containsCancellationException(
                        new IllegalStateException("ordinary failure")));
    }
}
