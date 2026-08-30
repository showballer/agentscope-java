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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    private DockerSandboxState ownedState() {
        DockerSandboxState state = new DockerSandboxState();
        state.setContainerId("container-1");
        state.setContainerName("container-1");
        state.setContainerOwned(true);
        state.setWorkspaceSpec(new WorkspaceSpec());
        return state;
    }

    private Path fakeDocker(String body) throws Exception {
        Path script = tempDir.resolve("fake-docker-" + System.nanoTime());
        Files.writeString(script, "#!/bin/sh\n" + body);
        Files.setPosixFilePermissions(script, PosixFilePermissions.fromString("rwx------"));
        return script;
    }
}
