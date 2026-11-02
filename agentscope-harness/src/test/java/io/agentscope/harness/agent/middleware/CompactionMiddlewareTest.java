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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.agentscope.core.ReActAgent;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.middleware.ReasoningInput;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import io.agentscope.core.model.ToolSchema;
import io.agentscope.core.state.AgentState;
import io.agentscope.harness.agent.memory.compaction.CompactionConfig;
import io.agentscope.harness.agent.memory.compaction.ConversationCompactor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

class CompactionMiddlewareTest {

    @Test
    void successfulCompactionPreservesSystemMessageAndUpdatesOnlyCallScopedState() {
        Fixture fixture = new Fixture(successfulSummary());
        AgentState fallbackState =
                AgentState.builder().context(List.of(message("OTHER_SESSION"))).build();
        when(fixture.agent.getAgentState()).thenReturn(fallbackState);

        fixture.reason(fixture.recordingNext()).blockLast();

        assertEquals(1, fixture.requests.size());
        ReasoningInput forwarded = fixture.requests.get(0);
        assertNotSame(fixture.input, forwarded);
        assertSame(fixture.system, forwarded.messages().get(0));
        assertEquals(ConversationCompactor.SUMMARY_MSG_NAME, forwarded.messages().get(1).getName());
        assertEquals(fixture.conversation.subList(2, 4), forwarded.messages().subList(2, 4));
        assertEquals(forwarded.messages().subList(1, 4), fixture.state.getContext());
        assertSame(fixture.input.tools(), forwarded.tools());
        assertSame(fixture.input.options(), forwarded.options());
        assertEquals(5, fixture.input.messages().size(), "input must remain unchanged");
        assertEquals(
                "OTHER_SESSION",
                ((TextBlock) fallbackState.getContext().get(0).getContent().get(0)).getText());
        verify(fixture.agent, never()).getAgentState();
        verify(fixture.model, times(1)).stream(anyList(), isNull(), isNull());
    }

    @ParameterizedTest
    @MethodSource("unusableSummaries")
    void failedOrEmptySummaryPreservesOriginalInputAndState(Flux<ChatResponse> summary) {
        Fixture fixture = new Fixture(summary);

        fixture.reason(fixture.recordingNext()).blockLast();

        assertEquals(1, fixture.requests.size());
        assertSame(fixture.input, fixture.requests.get(0));
        assertEquals(fixture.conversation, fixture.state.getContext());
        assertSame(fixture.system, fixture.requests.get(0).messages().get(0));
        verify(fixture.model, times(1)).stream(anyList(), isNull(), isNull());
    }

    private static Stream<Flux<ChatResponse>> unusableSummaries() {
        return Stream.of(
                Flux.empty(),
                Flux.just(response(" \n\t ")),
                Flux.error(new IllegalStateException("summary unavailable")),
                Flux.concat(
                        successfulSummary(),
                        Flux.error(new IllegalStateException("summary stream failed"))));
    }

    @Test
    void synchronousCompactionFailureFallsBackWithoutChangingInputOrState() {
        Fixture fixture = new Fixture(successfulSummary());
        when(fixture.model.getContextWindowSize())
                .thenThrow(new IllegalStateException("model unavailable"));

        fixture.reason(fixture.recordingNext()).blockLast();

        assertEquals(1, fixture.requests.size());
        assertSame(fixture.input, fixture.requests.get(0));
        assertEquals(fixture.conversation, fixture.state.getContext());
        verify(fixture.model, never()).stream(anyList(), isNull(), isNull());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void downstreamAsyncFailurePropagatesWithoutRetryAfterSuccessfulOrSkippedCompaction(
            boolean compact) {
        Fixture fixture = new Fixture(compact ? successfulSummary() : Flux.empty());
        IllegalStateException downstreamFailure = new IllegalStateException("reasoning failed");

        IllegalStateException thrown =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                fixture.reason(
                                                input -> {
                                                    fixture.requests.add(input);
                                                    return Flux.error(downstreamFailure);
                                                })
                                        .blockLast());

        assertSame(downstreamFailure, thrown);
        assertEquals(1, fixture.requests.size());
        if (!compact) {
            assertSame(fixture.input, fixture.requests.get(0));
            assertEquals(fixture.conversation, fixture.state.getContext());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void downstreamSynchronousFailurePropagatesWithoutRetry(boolean compact) {
        Fixture fixture = new Fixture(compact ? successfulSummary() : Flux.empty());
        IllegalStateException downstreamFailure =
                new IllegalStateException("cannot start reasoning");

        IllegalStateException thrown =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                fixture.reason(
                                                input -> {
                                                    fixture.requests.add(input);
                                                    throw downstreamFailure;
                                                })
                                        .blockLast());

        assertSame(downstreamFailure, thrown);
        assertEquals(1, fixture.requests.size());
    }

    @Test
    void failureAfterDownstreamEmitsEventDoesNotDuplicatePartialResponse() {
        Fixture fixture = new Fixture(successfulSummary());
        AgentEvent event = mock(AgentEvent.class);
        IllegalStateException downstreamFailure =
                new IllegalStateException("reasoning stream failed");
        List<AgentEvent> received = new ArrayList<>();

        assertSame(
                downstreamFailure,
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                fixture.reason(
                                                input -> {
                                                    fixture.requests.add(input);
                                                    return Flux.concat(
                                                            Flux.just(event),
                                                            Flux.error(downstreamFailure));
                                                })
                                        .doOnNext(received::add)
                                        .blockLast()));

        assertEquals(List.of(event), received);
        assertEquals(1, fixture.requests.size());
    }

    @Test
    void summaryInterruptionPropagatesWithoutCallingDownstreamOrChangingState() {
        IllegalStateException cancelled =
                new IllegalStateException("cancelled", new InterruptedException("stop"));
        Fixture fixture = new Fixture(Flux.error(cancelled));

        assertSame(
                cancelled,
                assertThrows(
                        IllegalStateException.class,
                        () -> fixture.reason(fixture.recordingNext()).blockLast()));

        assertTrue(fixture.requests.isEmpty());
        assertEquals(fixture.conversation, fixture.state.getContext());
    }

    @Test
    void explicitSummaryCancellationPropagatesWithoutCallingDownstreamOrChangingState() {
        IllegalStateException cancelled =
                new IllegalStateException("cancelled", new CancellationException("stop"));
        Fixture fixture = new Fixture(Flux.error(cancelled));

        assertSame(
                cancelled,
                assertThrows(
                        IllegalStateException.class,
                        () -> fixture.reason(fixture.recordingNext()).blockLast()));

        assertTrue(fixture.requests.isEmpty());
        assertEquals(fixture.conversation, fixture.state.getContext());
    }

    @Test
    void reactiveCancellationCancelsSummaryWithoutCallingDownstreamOrChangingState() {
        AtomicBoolean summaryCancelled = new AtomicBoolean();
        Fixture fixture =
                new Fixture(
                        Flux.<ChatResponse>never().doOnCancel(() -> summaryCancelled.set(true)));

        Disposable subscription = fixture.reason(fixture.recordingNext()).subscribe();
        subscription.dispose();

        assertTrue(summaryCancelled.get());
        assertTrue(fixture.requests.isEmpty());
        assertEquals(fixture.conversation, fixture.state.getContext());
    }

    @Test
    void belowThresholdForwardsOriginalInputWithoutCallingSummaryModel() {
        Fixture fixture = new Fixture(successfulSummary());
        ReasoningInput shortInput =
                new ReasoningInput(List.of(fixture.system, message("SHORT")), null, null);

        fixture.middleware
                .onReasoning(
                        fixture.agent, fixture.runtimeContext, shortInput, fixture.recordingNext())
                .blockLast();

        assertEquals(List.of(shortInput), fixture.requests);
        assertSame(shortInput, fixture.requests.get(0));
        assertEquals(fixture.conversation, fixture.state.getContext());
        verify(fixture.model, never()).stream(anyList(), isNull(), isNull());
    }

    private static Flux<ChatResponse> successfulSummary() {
        return Flux.just(response("SUMMARY"));
    }

    private static ChatResponse response(String text) {
        return ChatResponse.builder()
                .content(List.of(TextBlock.builder().text(text).build()))
                .build();
    }

    private static Msg message(String text) {
        return Msg.builder()
                .role(MsgRole.USER)
                .content(TextBlock.builder().text(text).build())
                .build();
    }

    private static final class Fixture {
        private final ReActAgent agent = mock(ReActAgent.class);
        private final Model model = mock(Model.class);
        private final Msg system =
                Msg.builder()
                        .role(MsgRole.SYSTEM)
                        .content(TextBlock.builder().text("SYSTEM").build())
                        .build();
        private final List<Msg> conversation =
                List.of(message("OLD_1"), message("OLD_2"), message("TAIL_1"), message("TAIL_2"));
        private final AgentState state = AgentState.builder().context(conversation).build();
        private final RuntimeContext runtimeContext =
                RuntimeContext.builder().agentState(state).build();
        private final List<ReasoningInput> requests = new ArrayList<>();
        private final CompactionMiddleware middleware;
        private final ReasoningInput input;

        private Fixture(Flux<ChatResponse> summary) {
            when(agent.getName()).thenReturn("compaction-test");
            when(model.stream(anyList(), isNull(), isNull())).thenReturn(summary);
            CompactionConfig config =
                    CompactionConfig.builder()
                            .triggerMessages(3)
                            .triggerTokens(0)
                            .keepMessages(2)
                            .keepTokens(0)
                            .flushBeforeCompact(false)
                            .offloadBeforeCompact(false)
                            .truncateArgs(null)
                            .prune(null)
                            .build();
            middleware = new CompactionMiddleware(null, model, config);
            List<Msg> messages = new ArrayList<>();
            messages.add(system);
            messages.addAll(conversation);
            input =
                    new ReasoningInput(
                            messages,
                            List.of(mock(ToolSchema.class)),
                            GenerateOptions.builder().temperature(0.2).build());
        }

        private Flux<AgentEvent> reason(Function<ReasoningInput, Flux<AgentEvent>> next) {
            return middleware.onReasoning(agent, runtimeContext, input, next);
        }

        private Function<ReasoningInput, Flux<AgentEvent>> recordingNext() {
            return input -> {
                requests.add(input);
                return Flux.empty();
            };
        }
    }
}
