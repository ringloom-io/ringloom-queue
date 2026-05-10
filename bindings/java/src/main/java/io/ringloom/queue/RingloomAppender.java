// SPDX-License-Identifier: Apache-2.0

package io.ringloom.queue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single-writer append handle for a {@link RingloomQueue}.
 *
 * <p>An appender owns the queue's native appender lease. Do not call append methods concurrently on
 * the same appender instance.</p>
 */
public final class RingloomAppender implements AutoCloseable {

    private final RingloomQueue queue;
    private final MemorySegment nativeHandle;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    RingloomAppender(RingloomQueue queue, MemorySegment nativeHandle) {
        this.queue = queue;
        this.nativeHandle = nativeHandle;
    }

    /**
     * Appends an owned Java byte array and returns the assigned queue index.
     *
     * @param payload payload bytes to append
     * @return public queue index assigned by the native appender
     */
    public long append(byte[] payload) {
        Objects.requireNonNull(payload, "payload");
        if (payload.length == 0) {
            throw new IllegalArgumentException("payload must not be empty");
        }
        ensureOpen();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativePayload = arena.allocateFrom(
                ValueLayout.JAVA_BYTE,
                payload
            );
            return appendNative(nativePayload, payload.length, arena);
        }
    }

    /**
     * Appends bytes from an off-heap native memory segment and returns the assigned queue index.
     *
     * <p>The supplied segment must expose a valid native address. Heap segments are not valid native
     * pointers for this API.</p>
     *
     * @param payload off-heap payload segment to append
     * @return public queue index assigned by the native appender
     */
    public long append(MemorySegment payload) {
        Objects.requireNonNull(payload, "payload");
        if (payload.byteSize() == 0) {
            throw new IllegalArgumentException("payload must not be empty");
        }
        ensureOpen();

        try (Arena arena = Arena.ofConfined()) {
            return appendNative(payload, payload.byteSize(), arena);
        }
    }

    /**
     * Encodes a Java string as UTF-8 and appends it to the queue.
     *
     * @param payload string payload to append
     * @return public queue index assigned by the native appender
     */
    public long appendString(String payload) {
        Objects.requireNonNull(payload, "payload");
        return append(payload.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Closes the native appender handle and releases the appender lease.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        RingloomNative.appenderClose(nativeHandle);
    }

    void ensureOpen() {
        queue.ensureOpen();
        if (closed.get()) {
            throw new IllegalStateException("RingloomAppender is closed");
        }
    }

    private long appendNative(
        MemorySegment payload,
        long payloadLen,
        Arena arena
    ) {
        MemorySegment outIndex = arena.allocate(ValueLayout.JAVA_LONG);
        int status = RingloomNative.appenderAppend(
            nativeHandle,
            payload,
            payloadLen,
            outIndex
        );
        RingloomNative.throwForStatus("ringloom_appender_append", status);
        return outIndex.get(ValueLayout.JAVA_LONG, 0);
    }
}
