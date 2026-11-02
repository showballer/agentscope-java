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
package io.agentscope.harness.agent.sandbox.impl.docker;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.harness.agent.sandbox.WorkspaceSpec;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DockerSandboxShutdownTest {

    @TempDir Path tempDir;

    @AfterEach
    void clearDockerOverride() {
        System.clearProperty(DockerSandbox.DOCKER_CLI_PROPERTY);
        System.clearProperty(DockerSandbox.DOCKER_START_TIMEOUT_SECONDS_PROPERTY);
    }

    @Test
    void shutdownSucceedsWhenForceRemoveWinsAfterStopFailure() throws Exception {
        Path state = Files.writeString(tempDir.resolve("container-state"), "running");
        Path docker =
                fakeDocker(
                        """
                        case "$1" in
                          stop) exit 1 ;;
                          rm) rm -f '%s'; exit 0 ;;
                          inspect)
                            if [ -f '%s' ]; then echo true; exit 0; fi
                            echo 'Error: No such object: container-1' >&2; exit 1 ;;
                        esac
                        exit 2
                        """
                                .formatted(state, state));
        System.setProperty(DockerSandbox.DOCKER_CLI_PROPERTY, docker.toString());
        DockerSandboxState sandboxState = ownedState();

        assertDoesNotThrow(() -> new DockerSandbox(sandboxState).shutdown());
        assertNull(sandboxState.getContainerId());
    }

    @Test
    void shutdownFailsClosedWhenContainerIsStillRunning() throws Exception {
        Path docker =
                fakeDocker(
                        """
                        case "$1" in
                          stop|rm) exit 1 ;;
                          inspect) echo true; exit 0 ;;
                        esac
                        exit 2
                        """);
        System.setProperty(DockerSandbox.DOCKER_CLI_PROPERTY, docker.toString());

        assertThrows(Exception.class, () -> new DockerSandbox(ownedState()).shutdown());
    }

    @Test
    void shutdownFailsClosedWhenContainerStoppedButRemoveFailed() throws Exception {
        Path docker =
                fakeDocker(
                        """
                        case "$1" in
                          stop) exit 0 ;;
                          rm) exit 1 ;;
                          inspect) echo false; exit 0 ;;
                        esac
                        exit 2
                        """);
        System.setProperty(DockerSandbox.DOCKER_CLI_PROPERTY, docker.toString());
        DockerSandboxState state = ownedState();

        assertThrows(Exception.class, () -> new DockerSandbox(state).shutdown());
        assertTrue(state.getContainerId().contains("container-1"));
        assertTrue(state.getContainerName().contains("container-1"));
    }

    @Test
    void shutdownFailsClosedWhenDaemonCannotConfirmState() throws Exception {
        Path docker =
                fakeDocker(
                        """
                        case "$1" in
                          stop|rm) exit 1 ;;
                          inspect) echo 'Cannot connect to the Docker daemon' >&2; exit 1 ;;
                        esac
                        exit 2
                        """);
        System.setProperty(DockerSandbox.DOCKER_CLI_PROPERTY, docker.toString());

        assertThrows(Exception.class, () -> new DockerSandbox(ownedState()).shutdown());
    }

    @Test
    void shutdownFailsClosedWhenInspectReturnsEmptySuccess() throws Exception {
        Path docker =
                fakeDocker(
                        """
                        case "$1" in
                          stop|rm) exit 1 ;;
                          inspect) exit 0 ;;
                        esac
                        exit 2
                        """);
        System.setProperty(DockerSandbox.DOCKER_CLI_PROPERTY, docker.toString());

        assertThrows(Exception.class, () -> new DockerSandbox(ownedState()).shutdown());
    }

    @Test
    void shutdownFailsClosedWhenInspectReturnsMalformedSuccess() throws Exception {
        Path docker =
                fakeDocker(
                        """
                        case "$1" in
                          stop|rm) exit 1 ;;
                          inspect) echo warning-not-a-state; exit 0 ;;
                        esac
                        exit 2
                        """);
        System.setProperty(DockerSandbox.DOCKER_CLI_PROPERTY, docker.toString());

        assertThrows(Exception.class, () -> new DockerSandbox(ownedState()).shutdown());
    }

    @Test
    void shutdownUsesOwnedContainerNameWhenIdWasNeverObserved() throws Exception {
        Path stateFile = Files.writeString(tempDir.resolve("container-by-name"), "running");
        Path calls = tempDir.resolve("docker-calls");
        Path docker = cleanupDocker(stateFile, calls);
        System.setProperty(DockerSandbox.DOCKER_CLI_PROPERTY, docker.toString());
        DockerSandboxState state = newSandboxState("shutdown-by-name");
        state.setContainerName("agentscope-sandbox-shutdown-by-name");

        assertDoesNotThrow(() -> new DockerSandbox(state).shutdown());

        assertFalse(Files.exists(stateFile));
        assertNull(state.getContainerId());
        assertNull(state.getContainerName());
        String invocations = Files.readString(calls);
        assertTrue(invocations.contains("stop --time=30 agentscope-sandbox-shutdown-by-name"));
        assertTrue(invocations.contains("rm --force agentscope-sandbox-shutdown-by-name"));
    }

    @Test
    void shutdownByNameFailsClosedWhenContainerIsStillRunning() throws Exception {
        Path docker =
                fakeDocker(
                        """
                        case "$1" in
                          stop|rm) exit 1 ;;
                          inspect) echo true; exit 0 ;;
                        esac
                        exit 2
                        """);
        System.setProperty(DockerSandbox.DOCKER_CLI_PROPERTY, docker.toString());
        DockerSandboxState state = newSandboxState("still-running-by-name");
        state.setContainerName("agentscope-sandbox-still-running-by-name");

        assertThrows(Exception.class, () -> new DockerSandbox(state).shutdown());
        assertNull(state.getContainerId());
        assertTrue(state.getContainerName().contains("still-running-by-name"));
    }

    @Test
    void failedStartCleansContainerByNameWhenRunReturnsNoId() throws Exception {
        assertFailedStartCleansNamedContainer("exit 0", "no-id");
    }

    @Test
    void failedStartCleansContainerByNameWhenRunFailsAfterDaemonCreation() throws Exception {
        assertFailedStartCleansNamedContainer("echo run-failed >&2; exit 17", "failed-run");
    }

    @Test
    void timedOutStartCleansContainerByName() throws Exception {
        System.setProperty(DockerSandbox.DOCKER_START_TIMEOUT_SECONDS_PROPERTY, "1");
        assertFailedStartCleansNamedContainer("exec sleep 5", "timed-out-run");
    }

    private DockerSandboxState ownedState() {
        DockerSandboxState state = new DockerSandboxState();
        state.setContainerId("container-1");
        state.setContainerName("container-1");
        state.setContainerOwned(true);
        state.setWorkspaceSpec(new WorkspaceSpec());
        return state;
    }

    private void assertFailedStartCleansNamedContainer(String runResult, String sessionId)
            throws Exception {
        Path stateFile = tempDir.resolve("container-state-" + sessionId);
        Path calls = tempDir.resolve("docker-calls-" + sessionId);
        Path docker =
                fakeDocker(
                        """
                        printf '%%s\\n' "$*" >> '%s'
                        case "$1" in
                          run) touch '%s'; %s ;;
                          stop) exit 0 ;;
                          rm) rm -f '%s'; exit 0 ;;
                          inspect)
                            if [ -f '%s' ]; then echo true; exit 0; fi
                            echo 'Error: No such object' >&2; exit 1 ;;
                        esac
                        exit 2
                        """
                                .formatted(calls, stateFile, runResult, stateFile, stateFile));
        System.setProperty(DockerSandbox.DOCKER_CLI_PROPERTY, docker.toString());
        DockerSandboxState state = newSandboxState(sessionId);

        assertThrows(Exception.class, () -> new DockerSandbox(state).start());

        assertFalse(Files.exists(stateFile));
        assertNull(state.getContainerId());
        assertNull(state.getContainerName());
        String invocations = Files.readString(calls);
        String expectedName = "agentscope-sandbox-" + sessionId;
        assertTrue(invocations.contains("run -d --name " + expectedName));
        assertTrue(invocations.contains("rm --force " + expectedName));
        assertTrue(invocations.contains("inspect -f {{.State.Running}} " + expectedName));
    }

    private DockerSandboxState newSandboxState(String sessionId) {
        DockerSandboxState state = new DockerSandboxState();
        state.setSessionId(sessionId);
        state.setImage("sandbox:test");
        state.setWorkspaceRoot("/workspace");
        state.setContainerOwned(true);
        state.setWorkspaceSpec(new WorkspaceSpec());
        return state;
    }

    private Path cleanupDocker(Path stateFile, Path calls) throws Exception {
        return fakeDocker(
                """
                printf '%%s\\n' "$*" >> '%s'
                case "$1" in
                  stop) exit 0 ;;
                  rm) rm -f '%s'; exit 0 ;;
                  inspect)
                    if [ -f '%s' ]; then echo true; exit 0; fi
                    echo 'Error: No such object' >&2; exit 1 ;;
                esac
                exit 2
                """
                        .formatted(calls, stateFile, stateFile));
    }

    private Path fakeDocker(String body) throws Exception {
        Path script = tempDir.resolve("fake-docker-" + System.nanoTime());
        Files.writeString(script, "#!/bin/sh\n" + body);
        Files.setPosixFilePermissions(script, PosixFilePermissions.fromString("rwx------"));
        return script;
    }
}
