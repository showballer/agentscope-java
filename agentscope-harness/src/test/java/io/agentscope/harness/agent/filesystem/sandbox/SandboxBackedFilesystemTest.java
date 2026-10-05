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
package io.agentscope.harness.agent.filesystem.sandbox;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.harness.agent.filesystem.model.FileDownloadResponse;
import io.agentscope.harness.agent.filesystem.model.FileUploadResponse;
import io.agentscope.harness.agent.sandbox.ExecResult;
import io.agentscope.harness.agent.sandbox.Sandbox;
import io.agentscope.harness.agent.sandbox.SandboxFileTransfer;
import io.agentscope.harness.agent.sandbox.SandboxInvocationKey;
import io.agentscope.harness.agent.sandbox.SandboxState;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SandboxBackedFilesystemTest {

    private static final RuntimeContext RT = RuntimeContext.empty();

    @Test
    void downloadFiles_decodesWrappedBase64Output() {
        byte[] expected = new byte[] {1, 2, 3, 4, 5, 6};
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        FakeSandbox sandbox = new FakeSandbox(new ExecResult(0, "AQID\nBAUG", "", false));
        filesystem.setSandbox(sandbox);

        List<FileDownloadResponse> responses =
                filesystem.downloadFiles(RT, List.of("/tmp/data.bin"));

        assertEquals("base64 '/tmp/data.bin'", sandbox.lastCommand);
        assertEquals(1, responses.size());
        assertTrue(responses.get(0).isSuccess());
        assertEquals("/tmp/data.bin", responses.get(0).path());
        assertArrayEquals(expected, responses.get(0).content());
    }

    @Test
    void downloadFiles_decodesEmptyPayloadWhenStdoutIsNull() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        FakeSandbox sandbox = new FakeSandbox(new ExecResult(0, null, "", false));
        filesystem.setSandbox(sandbox);

        List<FileDownloadResponse> responses =
                filesystem.downloadFiles(RT, List.of("/tmp/empty.bin"));

        assertEquals("base64 '/tmp/empty.bin'", sandbox.lastCommand);
        assertEquals(1, responses.size());
        assertTrue(responses.get(0).isSuccess());
        assertEquals("/tmp/empty.bin", responses.get(0).path());
        assertArrayEquals(new byte[0], responses.get(0).content());
    }

    @Test
    void downloadFiles_returnsFailureWhenCommandFails() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        FakeSandbox sandbox = new FakeSandbox(new ExecResult(1, "", "boom", false));
        filesystem.setSandbox(sandbox);

        List<FileDownloadResponse> responses =
                filesystem.downloadFiles(RT, List.of("/tmp/fail.bin"));

        assertEquals("base64 '/tmp/fail.bin'", sandbox.lastCommand);
        assertEquals(1, responses.size());
        assertTrue(!responses.get(0).isSuccess());
        assertEquals("/tmp/fail.bin", responses.get(0).path());
        assertEquals("[stderr] boom", responses.get(0).error());
    }

    @Test
    void uploadFiles_prefersNativeTransferWhenSupported() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        FakeTransferSandbox sandbox = new FakeTransferSandbox("/workspace");
        filesystem.setSandbox(sandbox);

        List<FileUploadResponse> responses =
                filesystem.uploadFiles(
                        RT, List.of(Map.entry("/workspace/a.txt", new byte[] {7, 8})));

        assertTrue(responses.get(0).isSuccess());
        assertArrayEquals(new byte[] {7, 8}, sandbox.uploaded.get("/workspace/a.txt"));
        assertEquals(null, sandbox.lastCommand);
    }

    @Test
    void uploadFiles_fallsBackToExecForUnsupportedPaths() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        FakeTransferSandbox sandbox = new FakeTransferSandbox("/workspace");
        filesystem.setSandbox(sandbox);

        List<FileUploadResponse> responses =
                filesystem.uploadFiles(RT, List.of(Map.entry("/etc/other.txt", new byte[] {1})));

        assertTrue(responses.get(0).isSuccess());
        assertTrue(sandbox.uploaded.isEmpty());
        assertTrue(sandbox.lastCommand.contains("base64 -d > '/etc/other.txt'"));
    }

    @Test
    void downloadFiles_prefersNativeTransferWhenSupported() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        FakeTransferSandbox sandbox = new FakeTransferSandbox("/workspace");
        sandbox.uploaded.put("/workspace/b.bin", new byte[] {9, 9});
        filesystem.setSandbox(sandbox);

        List<FileDownloadResponse> responses =
                filesystem.downloadFiles(RT, List.of("/workspace/b.bin"));

        assertTrue(responses.get(0).isSuccess());
        assertArrayEquals(new byte[] {9, 9}, responses.get(0).content());
        assertEquals(null, sandbox.lastCommand);
    }

    @Test
    void uploadFiles_reportsNativeTransferFailure() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        FakeTransferSandbox sandbox = new FakeTransferSandbox("/workspace");
        sandbox.failTransfers = true;
        filesystem.setSandbox(sandbox);

        List<FileUploadResponse> responses =
                filesystem.uploadFiles(RT, List.of(Map.entry("/workspace/c.txt", new byte[] {1})));

        assertTrue(!responses.get(0).isSuccess());
        assertEquals("transfer down", responses.get(0).error());
    }

    @Test
    void invocationBindingsRouteAndClearOverlappingCallsIndependently() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        RuntimeContext firstContext =
                RuntimeContext.builder()
                        .put(SandboxInvocationKey.class, new SandboxInvocationKey(1))
                        .build();
        RuntimeContext secondContext =
                RuntimeContext.builder()
                        .put(SandboxInvocationKey.class, new SandboxInvocationKey(2))
                        .build();
        // AgentScope creates derived RuntimeContexts during execution. They copy typed values but
        // are not the same context object, so routing must follow the invocation key rather than
        // RuntimeContext identity.
        RuntimeContext firstDerivedContext = RuntimeContext.builder(firstContext).build();
        RuntimeContext secondDerivedContext = RuntimeContext.builder(secondContext).build();
        FakeSandbox firstSandbox = new FakeSandbox(new ExecResult(0, "first", "", false));
        FakeSandbox secondSandbox = new FakeSandbox(new ExecResult(0, "second", "", false));

        filesystem.setSandbox(firstContext, firstSandbox);
        filesystem.setSandbox(secondContext, secondSandbox);

        assertEquals("first", filesystem.execute(firstDerivedContext, "from-first", null).output());
        assertEquals(
                "second", filesystem.execute(secondDerivedContext, "from-second", null).output());
        assertEquals("from-first", firstSandbox.lastCommand);
        assertEquals("from-second", secondSandbox.lastCommand);

        filesystem.clearSandbox(firstContext, firstSandbox);

        assertNull(filesystem.getSandbox(firstDerivedContext));
        assertSame(secondSandbox, filesystem.getSandbox(secondDerivedContext));
    }

    private static final class FakeTransferSandbox extends BaseFakeSandbox
            implements SandboxFileTransfer {

        private final String rootPrefix;
        private final Map<String, byte[]> uploaded = new HashMap<>();
        private boolean failTransfers;

        private FakeTransferSandbox(String root) {
            super(new ExecResult(0, "", "", false));
            this.rootPrefix = root + "/";
        }

        @Override
        public boolean supportsFileTransfer(String absolutePath) {
            return absolutePath.startsWith(rootPrefix);
        }

        @Override
        public void uploadFile(String absolutePath, byte[] content) throws Exception {
            if (failTransfers) {
                throw new IllegalStateException("transfer down");
            }
            uploaded.put(absolutePath, content);
        }

        @Override
        public byte[] downloadFile(String absolutePath) throws Exception {
            if (failTransfers) {
                throw new IllegalStateException("transfer down");
            }
            return uploaded.get(absolutePath);
        }
    }

    private static final class FakeSandbox extends BaseFakeSandbox {

        private FakeSandbox(ExecResult execResult) {
            super(execResult);
        }
    }

    private static class BaseFakeSandbox implements Sandbox {

        private final ExecResult execResult;
        protected String lastCommand;

        protected BaseFakeSandbox(ExecResult execResult) {
            this.execResult = execResult;
        }

        @Override
        public void start() {}

        @Override
        public void stop() {}

        @Override
        public void shutdown() {}

        @Override
        public void close() {}

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public SandboxState getState() {
            return null;
        }

        @Override
        public ExecResult exec(
                RuntimeContext runtimeContext, String command, Integer timeoutSeconds) {
            this.lastCommand = command;
            return execResult;
        }

        @Override
        public InputStream persistWorkspace() {
            return InputStream.nullInputStream();
        }

        @Override
        public void hydrateWorkspace(InputStream archive) {}
    }
}
