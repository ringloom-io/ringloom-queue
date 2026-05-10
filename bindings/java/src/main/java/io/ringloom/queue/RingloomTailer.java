// SPDX-License-Identifier: Apache-2.0

package io.ringloom.queue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Independent non-blocking read cursor for a {@link RingloomQueue}.
 *
 * <p>A tailer maintains its own borrowed message view state and can be polled independently from
 * other tailers on the same queue.</p>
 */
public final class RingloomTailer implements AutoCloseable {

    private final RingloomQueue queue;
    private final MemorySegment nativeHandle;
    private final Arena arena;
    private final MemorySegment nativeView;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    RingloomTailer(RingloomQueue queue, MemorySegment nativeHandle) {
        this.queue = queue;
        this.nativeHandle = nativeHandle;
        this.arena = Arena.ofShared();
        this.nativeView = arena.allocate(RingloomNative.MESSAGE_VIEW_SIZE, 8);
        this.nativeView.fill((byte) 0);
        this.nativeView.set(
            ValueLayout.JAVA_INT,
            RingloomNative.MESSAGE_VIEW_SIZE_OFFSET,
            (int) RingloomNative.MESSAGE_VIEW_SIZE
        );
    }

    /**
     * Polls once and writes a borrowed message view into {@code out} when a message is available.
     *
     * @param out reusable borrowed view that will be updated when a message is available
     * @return {@code true} when a message was read, {@code false} when the tailer is currently empty
     */
    public boolean poll(RingloomMessageView out) {
        ensureOpen();
        out.clear();
        nativeView.set(
            ValueLayout.JAVA_INT,
            RingloomNative.MESSAGE_VIEW_SIZE_OFFSET,
            (int) RingloomNative.MESSAGE_VIEW_SIZE
        );
        int status = RingloomNative.tailerPoll(nativeHandle, nativeView);
        if (status == RingloomStatus.OK_NOT_READY) {
            return false;
        }
        RingloomNative.throwForStatus("ringloom_tailer_poll", status);

        long index = nativeView.get(
            ValueLayout.JAVA_LONG,
            RingloomNative.MESSAGE_VIEW_INDEX_OFFSET
        );
        long payloadLength = nativeView.get(
            ValueLayout.JAVA_LONG,
            RingloomNative.MESSAGE_VIEW_DATA_LEN_OFFSET
        );
        MemorySegment payloadAddress = nativeView.get(
            RingloomNative.ADDRESS,
            RingloomNative.MESSAGE_VIEW_DATA_OFFSET
        );
        MemorySegment payload =
            payloadAddress.address() == 0
                ? MemorySegment.NULL
                : payloadAddress.reinterpret(payloadLength);
        out.set(index, payload, payloadLength);
        return true;
    }

    /**
     * Polls once and returns a newly allocated borrowed message view when data is available.
     *
     * @return optional borrowed message view
     */
    public Optional<RingloomMessageView> poll() {
        RingloomMessageView view = new RingloomMessageView();
        return poll(view) ? Optional.of(view) : Optional.empty();
    }

    /**
     * Polls once and returns an owned message copy when data is available.
     *
     * @return optional owned message copy
     */
    public Optional<RingloomMessage> pollCopy() {
        RingloomMessageView view = new RingloomMessageView();
        if (!poll(view)) {
            return Optional.empty();
        }
        return Optional.of(
            new RingloomMessage(view.index(), view.payloadBytes())
        );
    }

    /**
     * Drives bounded read-side prefetch work for this tailer.
     *
     * @param maxWorkUnits maximum amount of prefetch work to attempt
     * @return summary of whether work was available or completed
     */
    public StepResult prefetchPoll(int maxWorkUnits) {
        ensureOpen();
        if (maxWorkUnits < 0) {
            throw new IllegalArgumentException(
                "maxWorkUnits must be non-negative"
            );
        }
        try (Arena callArena = Arena.ofConfined()) {
            MemorySegment outStep = callArena.allocate(ValueLayout.JAVA_INT);
            int status = RingloomNative.tailerPrefetchPoll(
                nativeHandle,
                maxWorkUnits,
                outStep
            );
            RingloomNative.throwForStatus(
                "ringloom_tailer_prefetch_poll",
                status
            );
            return RingloomNative.readStepResult(outStep);
        }
    }

    /**
     * Closes the native tailer handle and invalidates any borrowed message views produced by it.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            RingloomNative.tailerClose(nativeHandle);
        } finally {
            arena.close();
        }
    }

    void ensureOpen() {
        queue.ensureOpen();
        if (closed.get()) {
            throw new IllegalStateException("RingloomTailer is closed");
        }
    }
}
