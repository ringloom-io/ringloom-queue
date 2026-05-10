// SPDX-License-Identifier: Apache-2.0

package io.ringloom.queue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.junit.jupiter.api.Test;

final class RingloomQueueSmokeTest {

    @Test
    void nativeAbiVersionMatches() {
        assertEquals(RingloomNative.ABI_VERSION, RingloomNative.abiVersion());
    }

    @Test
    void appendsAndPollsRawBytes() throws IOException {
        Path dir = Files.createTempDirectory("ringloom-queue-java-");
        try {
            QueueConfig config = QueueConfig.create(dir).withRollSchemeName(
                "TEST4_SECONDLY"
            );
            byte[] payload = "hello java".getBytes(StandardCharsets.UTF_8);

            try (
                RingloomQueue queue = RingloomQueue.open(config);
                RingloomAppender appender = queue.openAppender()
            ) {
                long index = appender.append(payload);

                try (RingloomTailer tailer = queue.openTailer(0)) {
                    RingloomMessageView view = new RingloomMessageView();
                    assertTrue(tailer.poll(view));
                    assertEquals(index, view.index());
                    assertArrayEquals(payload, view.payloadBytes());
                    assertFalse(tailer.poll(new RingloomMessageView()));
                    assertNotNull(tailer.prefetchPoll(1));
                    assertNotNull(queue.maintenancePoll(1));
                }
            }
        } finally {
            deleteRecursively(dir);
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
