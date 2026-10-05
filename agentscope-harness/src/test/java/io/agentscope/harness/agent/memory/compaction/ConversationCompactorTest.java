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
package io.agentscope.harness.agent.memory.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ThinkingBlock;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import io.agentscope.core.model.ToolSchema;
import io.agentscope.harness.agent.memory.MemoryFlushManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Tests message routing between summarization, memory flushing, and the preserved tail. */
class ConversationCompactorTest {

    /** Verifies that a prior summary is summarized again but is neither flushed nor retained. */
    @Test
    void compactIfNeeded_routesPriorSummaryToSummaryInputButNotFlushOrTail() {
        Fixture fixture = new Fixture();

        List<Msg> compacted =
                fixture.compact(
                        List.of(
                                summary("S1"),
                                message("M2"),
                                message("TAIL_1"),
                                message("TAIL_2")));

        assertTrue(fixture.model.prompt(0).contains("S1"));
        assertTrue(fixture.model.prompt(0).contains("M2"));
        assertEquals(List.of("M2"), texts(fixture.flushInputs.get(0)));
        assertEquals(3, compacted.size());
        assertTrue(compacted.get(0).getId().startsWith(ConversationCompactor.SUMMARY_MSG_NAME));
        assertEquals(ConversationCompactor.SUMMARY_MSG_NAME, compacted.get(0).getName());
        assertEquals(List.of("TAIL_1", "TAIL_2"), texts(compacted.subList(1, compacted.size())));
    }

    /** Verifies that multiple prior summaries in the prefix are replaced by one new summary. */
    @Test
    void compactIfNeeded_mergesMultiplePriorSummariesIntoOneReplacement() {
        Fixture fixture = new Fixture();

        List<Msg> compacted =
                fixture.compact(
                        List.of(
                                summary("S0"),
                                message("M1"),
                                summary("S1"),
                                message("M2"),
                                message("TAIL_1"),
                                message("TAIL_2")));

        String prompt = fixture.model.prompt(0);
        assertTrue(prompt.contains("S0"));
        assertTrue(prompt.contains("S1"));
        assertEquals(List.of("M1", "M2"), texts(fixture.flushInputs.get(0)));
        assertEquals(
                1,
                compacted.stream()
                        .filter(msg -> ConversationCompactor.SUMMARY_MSG_NAME.equals(msg.getName()))
                        .count());
    }

    /** Verifies that a summary-only prefix is not treated as empty conversation history. */
    @Test
    void compactIfNeeded_summaryOnlyPrefixDoesNotBecomeEmptyHistory() {
        Fixture fixture = new Fixture();

        List<Msg> compacted =
                fixture.compact(
                        List.of(summary("ONLY_OLD_SUMMARY"), message("TAIL_1"), message("TAIL_2")));

        assertTrue(fixture.model.prompt(0).contains("ONLY_OLD_SUMMARY"));
        assertTrue(fixture.flushInputs.get(0).isEmpty());
        assertTrue(text(compacted.get(0)).contains("ROUND_1"));
        assertFalse(text(compacted.get(0)).contains("No previous conversation history."));
    }

    /** Verifies that chained compaction summarizes the prior summary with the new prefix. */
    @Test
    void compactIfNeeded_chainedCompactionUsesPreviousSummaryAndNewPrefix() {
        Fixture fixture = new Fixture();
        List<Msg> first =
                fixture.compact(
                        List.of(
                                summary("S1"),
                                message("M2"),
                                message("TAIL_1"),
                                message("TAIL_2")));
        List<Msg> secondInput = new ArrayList<>(first);
        secondInput.add(message("M3"));
        secondInput.add(message("TAIL_3"));
        secondInput.add(message("TAIL_4"));

        List<Msg> second = fixture.compact(secondInput);

        assertTrue(fixture.model.prompt(1).contains("ROUND_1"));
        assertTrue(fixture.model.prompt(1).contains("M3"));
        assertFalse(text(second.get(0)).contains("ROUND_1"));
        assertTrue(text(second.get(0)).contains("ROUND_2"));
        assertEquals(List.of("TAIL_1", "TAIL_2", "M3"), texts(fixture.flushInputs.get(1)));
    }

    /** Verifies that disabling memory flush does not remove the prior summary from summarization. */
    @Test
    void compactIfNeeded_skipsFlushWhenDisabledAndStillUsesPriorSummary() {
        Fixture fixture = new Fixture(false);

        fixture.compact(
                List.of(summary("S1"), message("M2"), message("TAIL_1"), message("TAIL_2")));

        assertTrue(fixture.model.prompt(0).contains("S1"));
        assertTrue(fixture.model.prompt(0).contains("M2"));
        assertTrue(fixture.flushInputs.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("unusableSummaries")
    void compactIfNeeded_unusableSummaryDoesNotReplaceOriginalMessages(
            Flux<ChatResponse> response) {
        Fixture fixture = new Fixture(false);
        fixture.model.response = response;
        List<Msg> original =
                new ArrayList<>(
                        List.of(summary("PRIOR_SUMMARY"), message("TAIL_1"), message("TAIL_2")));
        List<Msg> before = List.copyOf(original);

        assertTrue(fixture.compactOptional(original).isEmpty());

        assertEquals(before, original);
        assertSame(before.get(0), original.get(0));
        assertEquals(1, fixture.model.inputs.size());
    }

    private static Stream<Flux<ChatResponse>> unusableSummaries() {
        return Stream.of(
                Flux.empty(),
                Flux.just(response(" \n\t ")),
                Flux.just(ChatResponse.builder().content(List.of()).build()),
                Flux.just(
                        ChatResponse.builder()
                                .content(
                                        List.of(
                                                ThinkingBlock.builder()
                                                        .thinking("thinking only")
                                                        .build()))
                                .build()),
                Flux.error(new IllegalStateException("summary unavailable")),
                Flux.concat(
                        Flux.just(response("partial summary")),
                        Flux.error(new IllegalStateException("summary stream interrupted"))));
    }

    @Test
    void compactIfNeeded_synchronousModelFailureDoesNotReplaceOriginalMessages() {
        Fixture fixture = new Fixture(false);
        fixture.model.synchronousFailure = new IllegalStateException("cannot start summary stream");

        assertTrue(
                fixture.compactOptional(
                                List.of(message("OLD"), message("TAIL_1"), message("TAIL_2")))
                        .isEmpty());
        assertEquals(1, fixture.model.inputs.size());
    }

    @Test
    void compactIfNeeded_failedSummaryDoesNotApplyPreparedToolPruningOrArgTruncation() {
        Fixture fixture = new Fixture(false);
        fixture.model.response = Flux.error(new IllegalStateException("summary failed"));
        String argument = "ARGUMENT".repeat(200);
        String output = "TOOL_OUTPUT".repeat(200);
        Msg call = toolCall(argument);
        Msg result = toolResult(output);
        List<Msg> original =
                new ArrayList<>(List.of(call, result, message("TAIL_1"), message("TAIL_2")));
        List<Msg> before = List.copyOf(original);
        CompactionConfig aggressiveConfig =
                CompactionConfig.builder()
                        .triggerMessages(3)
                        .triggerTokens(0)
                        .keepMessages(2)
                        .keepTokens(0)
                        .flushBeforeCompact(false)
                        .offloadBeforeCompact(false)
                        .truncateArgs(
                                CompactionConfig.TruncateArgsConfig.builder()
                                        .triggerMessages(1)
                                        .keepMessages(2)
                                        .keepTokens(0)
                                        .maxArgLength(20)
                                        .build())
                        .prune(
                                CompactionConfig.PruneConfig.builder()
                                        .protectTokens(0)
                                        .minimumTokens(1)
                                        .maxOutputChars(40)
                                        .build())
                        .build();

        List<Msg> prepared =
                fixture.compactor.pruneToolResults(
                        fixture.compactor.truncateArgs(
                                original, aggressiveConfig.getTruncateArgsConfig()),
                        aggressiveConfig.getPruneConfig());
        assertFalse(
                argument.equals(
                        ((ToolUseBlock) prepared.get(0).getContent().get(0))
                                .getInput()
                                .get("command")));
        assertFalse(output.equals(toolOutput(prepared.get(1))));

        assertTrue(
                fixture.compactor
                        .compactIfNeeded(
                                fixture.runtimeContext,
                                original,
                                aggressiveConfig,
                                "agent",
                                "session")
                        .block()
                        .isEmpty());

        assertEquals(before, original);
        assertSame(call, original.get(0));
        assertSame(result, original.get(1));
        assertEquals(argument, ((ToolUseBlock) call.getContent().get(0)).getInput().get("command"));
        assertEquals(output, toolOutput(result));
    }

    @Test
    void compactIfNeeded_keepsToolCallAndResultTogetherWhenCutoffFallsOnResult() {
        Fixture fixture = new Fixture(false);
        Msg call = toolCall("command");
        Msg result = toolResult("result");
        Msg tail = message("TAIL");

        List<Msg> compacted = fixture.compact(List.of(message("OLD"), call, result, tail));

        assertEquals(4, compacted.size());
        assertSame(call, compacted.get(1));
        assertSame(result, compacted.get(2));
        assertSame(tail, compacted.get(3));
        assertFalse(fixture.model.prompt(0).contains("tool_call"));
    }

    @Test
    void compactIfNeeded_propagatesWrappedInterruptionWithoutReplacement() {
        Fixture fixture = new Fixture(false);
        IllegalStateException cancelled =
                new IllegalStateException("cancelled", new InterruptedException("stop"));
        fixture.model.response = Flux.error(cancelled);

        assertSame(
                cancelled,
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                fixture.compactOptional(
                                        List.of(
                                                message("OLD"),
                                                message("TAIL_1"),
                                                message("TAIL_2")))));
    }

    @Test
    void compactIfNeeded_propagatesFlushInterruptionWithoutCallingSummarizer() {
        Fixture fixture = new Fixture();
        IllegalStateException cancelled =
                new IllegalStateException("cancelled", new InterruptedException("stop"));
        when(fixture.flushManager.flushMemories(any(RuntimeContext.class), anyList()))
                .thenReturn(Mono.error(cancelled));

        assertSame(
                cancelled,
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                fixture.compactOptional(
                                        List.of(
                                                message("OLD"),
                                                message("TAIL_1"),
                                                message("TAIL_2")))));
        assertTrue(fixture.model.inputs.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("cancellationFailures")
    void compactIfNeeded_propagatesOffloadCancellationWithoutCallingSummarizer(
            RuntimeException cancelled) {
        Fixture fixture = new Fixture(false, true);
        doThrow(cancelled)
                .when(fixture.flushManager)
                .offloadMessages(any(RuntimeContext.class), anyList(), anyString(), anyString());

        assertSame(
                cancelled,
                assertThrows(
                        RuntimeException.class,
                        () ->
                                fixture.compactOptional(
                                        List.of(
                                                message("OLD"),
                                                message("TAIL_1"),
                                                message("TAIL_2")))));
        assertTrue(fixture.model.inputs.isEmpty());
    }

    private static Stream<RuntimeException> cancellationFailures() {
        return Stream.of(
                new IllegalStateException("cancelled", new InterruptedException("stop")),
                new CancellationException("stop"));
    }

    @Test
    void compactIfNeeded_ordinaryFlushAndOffloadFailuresStillAllowValidSummary() {
        Fixture fixture = new Fixture(true, true);
        when(fixture.flushManager.flushMemories(any(RuntimeContext.class), anyList()))
                .thenReturn(Mono.error(new IllegalStateException("memory flush unavailable")));
        doThrow(new IllegalStateException("archive unavailable"))
                .when(fixture.flushManager)
                .offloadMessages(any(RuntimeContext.class), anyList(), anyString(), anyString());

        List<Msg> compacted =
                fixture.compact(List.of(message("OLD"), message("TAIL_1"), message("TAIL_2")));

        assertTrue(text(compacted.get(0)).contains("ROUND_1"));
        assertEquals(List.of("TAIL_1", "TAIL_2"), texts(compacted.subList(1, compacted.size())));
        assertEquals(1, fixture.model.inputs.size());
    }

    @Test
    void compactIfNeeded_successfulOffloadKeepsArchiveReferenceInSummary() {
        Fixture fixture = new Fixture(false, true);
        when(fixture.flushManager.resolveOffloadPath(
                        any(RuntimeContext.class), anyString(), anyString()))
                .thenReturn("/archive/session.jsonl");

        List<Msg> compacted =
                fixture.compact(List.of(message("OLD"), message("TAIL_1"), message("TAIL_2")));

        assertTrue(text(compacted.get(0)).contains("/archive/session.jsonl"));
        assertTrue(text(compacted.get(0)).contains("ROUND_1"));
    }

    private static ChatResponse response(String text) {
        return ChatResponse.builder()
                .content(List.of(TextBlock.builder().text(text).build()))
                .build();
    }

    private static Msg toolCall(String argument) {
        return Msg.builder()
                .role(MsgRole.ASSISTANT)
                .content(
                        ToolUseBlock.builder()
                                .id("call-1")
                                .name("execute")
                                .input(Map.of("command", argument))
                                .build())
                .build();
    }

    private static Msg toolResult(String output) {
        return Msg.builder()
                .role(MsgRole.TOOL)
                .content(
                        ToolResultBlock.builder()
                                .id("call-1")
                                .name("execute")
                                .output(List.of(TextBlock.builder().text(output).build()))
                                .build())
                .build();
    }

    private static String toolOutput(Msg message) {
        ToolResultBlock result = (ToolResultBlock) message.getContent().get(0);
        return ((TextBlock) result.getOutput().get(0)).getText();
    }

    /** Creates a regular user message. */
    private static Msg message(String text) {
        return Msg.builder()
                .role(MsgRole.USER)
                .content(TextBlock.builder().text(text).build())
                .build();
    }

    /** Creates a summary message injected by the compactor. */
    private static Msg summary(String text) {
        return Msg.builder()
                .role(MsgRole.USER)
                .name(ConversationCompactor.SUMMARY_MSG_NAME)
                .content(TextBlock.builder().text(text).build())
                .build();
    }

    /** Returns the text from the first content block of a message. */
    private static String text(Msg message) {
        return ((TextBlock) message.getContent().get(0)).getText();
    }

    /** Returns message texts for concise routing assertions. */
    private static List<String> texts(List<Msg> messages) {
        return messages.stream().map(ConversationCompactorTest::text).toList();
    }

    /** Holds isolated compactor dependencies and invocation records for each test. */
    private static final class Fixture {

        private final RecordingModel model = new RecordingModel();
        private final MemoryFlushManager flushManager = mock(MemoryFlushManager.class);
        private final RuntimeContext runtimeContext = mock(RuntimeContext.class);
        private final List<List<Msg>> flushInputs = new ArrayList<>();
        private final ConversationCompactor compactor;
        private final CompactionConfig config;

        /** Creates a fixture with memory flushing enabled. */
        private Fixture() {
            this(true);
        }

        /** Creates a fixture with the requested memory-flush setting. */
        private Fixture(boolean flushBeforeCompact) {
            this(flushBeforeCompact, false);
        }

        private Fixture(boolean flushBeforeCompact, boolean offloadBeforeCompact) {
            when(flushManager.flushMemories(any(RuntimeContext.class), anyList()))
                    .thenAnswer(
                            invocation -> {
                                List<Msg> input = invocation.getArgument(1);
                                flushInputs.add(List.copyOf(input));
                                return Mono.empty();
                            });
            when(flushManager.resolveOffloadPath(
                            any(RuntimeContext.class), anyString(), anyString()))
                    .thenReturn("");
            compactor = new ConversationCompactor(model, flushManager);
            config =
                    CompactionConfig.builder()
                            .triggerMessages(3)
                            .triggerTokens(0)
                            .keepMessages(2)
                            .keepTokens(0)
                            .summaryPrompt("SUMMARY_INPUT:\n{messages}")
                            .flushBeforeCompact(flushBeforeCompact)
                            .offloadBeforeCompact(offloadBeforeCompact)
                            .truncateArgs(null)
                            .prune(null)
                            .build();
        }

        /** Runs compaction and requires the supplied input to trigger it. */
        private List<Msg> compact(List<Msg> messages) {
            Optional<List<Msg>> result = compactOptional(messages);
            assertTrue(result.isPresent());
            return result.orElseThrow();
        }

        private Optional<List<Msg>> compactOptional(List<Msg> messages) {
            return compactor
                    .compactIfNeeded(runtimeContext, messages, config, "agent-id", "session-id")
                    .block();
        }
    }

    /** Records summarization prompts and returns a distinct fixed summary for each invocation. */
    private static final class RecordingModel implements Model {

        private final List<List<Msg>> inputs = new ArrayList<>();
        private Flux<ChatResponse> response;
        private RuntimeException synchronousFailure;

        /** Records the model input and emits a fixed text response for the current invocation. */
        @Override
        public Flux<ChatResponse> stream(
                List<Msg> messages, List<ToolSchema> tools, GenerateOptions options) {
            inputs.add(List.copyOf(messages));
            if (synchronousFailure != null) {
                throw synchronousFailure;
            }
            if (response != null) {
                return response;
            }
            String summary = "ROUND_" + inputs.size();
            return Flux.just(
                    ChatResponse.builder()
                            .id("summary-" + inputs.size())
                            .content(List.of(TextBlock.builder().text(summary).build()))
                            .build());
        }

        /** Returns the test model name. */
        @Override
        public String getModelName() {
            return "recording-model";
        }

        /** Returns the summarization prompt for the specified invocation. */
        private String prompt(int index) {
            return text(inputs.get(index).get(0));
        }
    }
}
