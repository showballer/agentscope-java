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
package io.agentscope.harness.agent.sandbox;

/**
 * Unique routing key for one subscribed agent invocation.
 *
 * <p>The key is stored as a typed {@link io.agentscope.core.agent.RuntimeContext} value. Derived
 * runtime contexts copy the same key, allowing sandbox-backed filesystem operations to remain
 * attached to the exact invocation even when AgentScope creates internal context forks.
 * Identity semantics are intentional; keys from different invocations never compare equal.
 */
public final class SandboxInvocationKey {

    private final long sequence;

    public SandboxInvocationKey(long sequence) {
        this.sequence = sequence;
    }

    public long sequence() {
        return sequence;
    }

    @Override
    public String toString() {
        return "sandbox-call-" + sequence;
    }
}
