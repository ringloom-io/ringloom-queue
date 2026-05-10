// SPDX-License-Identifier: Apache-2.0

package io.ringloom.queue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Open ringloom-queue handle.
 *
 * <p>A queue may have one active appender and many independent tailers. The native C ABI does not
 * create helper threads; applications that enable prefetcher or cleaner state should call
 * {@link #maintenancePoll(int)} or the narrower poll helpers from their own scheduling loop.</p>
 */
public final class RingloomQueue implements AutoCloseable {

    private final MemorySegment nativeHandle;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private RingloomQueue(MemorySegment nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    /**
     * Opens or creates a queue from the supplied configuration.
     *
     * @param config queue configuration
     * @return opened native queue wrapper
     */
    public static RingloomQueue open(QueueConfig config) {
        Objects.requireNonNull(config, "config");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment options = arena.allocate(
                RingloomNative.QUEUE_OPTIONS_SIZE,
                8
            );
            options.fill((byte) 0);

            byte[] dirBytes = config.dir().getBytes(StandardCharsets.UTF_8);
            MemorySegment dir = arena.allocateFrom(
                ValueLayout.JAVA_BYTE,
                dirBytes
            );
            MemorySegment rollSchemeName = MemorySegment.NULL;
            long rollSchemeNameLen = 0L;
            if (config.rollSchemeName() != null) {
                byte[] rollSchemeBytes = config
                    .rollSchemeName()
                    .getBytes(StandardCharsets.UTF_8);
                rollSchemeName = arena.allocateFrom(
                    ValueLayout.JAVA_BYTE,
                    rollSchemeBytes
                );
                rollSchemeNameLen = rollSchemeBytes.length;
            }

            options.set(
                ValueLayout.JAVA_INT,
                RingloomNative.OPTIONS_SIZE_OFFSET,
                (int) RingloomNative.QUEUE_OPTIONS_SIZE
            );
            options.set(
                RingloomNative.ADDRESS,
                RingloomNative.OPTIONS_DIR_OFFSET,
                dir
            );
            options.set(
                ValueLayout.JAVA_LONG,
                RingloomNative.OPTIONS_DIR_LEN_OFFSET,
                dirBytes.length
            );
            options.set(
                ValueLayout.JAVA_BOOLEAN,
                RingloomNative.OPTIONS_CREATE_OFFSET,
                config.create()
            );
            options.set(
                RingloomNative.ADDRESS,
                RingloomNative.OPTIONS_ROLL_SCHEME_NAME_OFFSET,
                rollSchemeName
            );
            options.set(
                ValueLayout.JAVA_LONG,
                RingloomNative.OPTIONS_ROLL_SCHEME_NAME_LEN_OFFSET,
                rollSchemeNameLen
            );
            options.set(
                ValueLayout.JAVA_BOOLEAN,
                RingloomNative.OPTIONS_ENABLE_PREFETCHER_OFFSET,
                config.enablePrefetcher()
            );
            options.set(
                ValueLayout.JAVA_BOOLEAN,
                RingloomNative.OPTIONS_ENABLE_CLEANER_OFFSET,
                config.enableCleaner()
            );
            options.set(
                ValueLayout.JAVA_BOOLEAN,
                RingloomNative.OPTIONS_SPAWN_HELPER_THREADS_OFFSET,
                config.spawnHelperThreads()
            );
            if (config.retentionCycles() != null) {
                options.set(
                    ValueLayout.JAVA_INT,
                    RingloomNative.OPTIONS_RETENTION_CYCLES_OFFSET,
                    config.retentionCycles()
                );
                options.set(
                    ValueLayout.JAVA_BOOLEAN,
                    RingloomNative.OPTIONS_RETENTION_CYCLES_SET_OFFSET,
                    true
                );
            }

            MemorySegment outQueue = arena.allocate(RingloomNative.ADDRESS);
            outQueue.set(RingloomNative.ADDRESS, 0, MemorySegment.NULL);
            int status = RingloomNative.queueOpen(options, outQueue);
            RingloomNative.throwForStatus("ringloom_queue_open", status);
            return new RingloomQueue(outQueue.get(RingloomNative.ADDRESS, 0));
        }
    }

    /**
     * Convenience factory that opens or creates a queue directory with default settings.
     *
     * @param dir queue directory path
     * @return opened native queue wrapper
     */
    public static RingloomQueue create(String dir) {
        return open(QueueConfig.create(dir));
    }

    /**
     * Opens the queue's single native appender lease.
     *
     * @return appender handle bound to this queue
     */
    public RingloomAppender openAppender() {
        ensureOpen();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outAppender = arena.allocate(RingloomNative.ADDRESS);
            outAppender.set(RingloomNative.ADDRESS, 0, MemorySegment.NULL);
            int status = RingloomNative.appenderOpen(nativeHandle, outAppender);
            RingloomNative.throwForStatus("ringloom_appender_open", status);
            return new RingloomAppender(
                this,
                outAppender.get(RingloomNative.ADDRESS, 0)
            );
        }
    }

    /**
     * Opens an independent tailer positioned at the first message at or after {@code startIndex}.
     *
     * <p>When a queue is empty and {@code startIndex} is {@code 0}, applications typically append at
     * least one message before opening a tailer.</p>
     *
     * @param startIndex public queue index to start from
     * @return tailer handle bound to this queue
     */
    public RingloomTailer openTailer(long startIndex) {
        ensureOpen();
        if (startIndex < 0) {
            throw new IllegalArgumentException(
                "startIndex must be non-negative"
            );
        }
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outTailer = arena.allocate(RingloomNative.ADDRESS);
            outTailer.set(RingloomNative.ADDRESS, 0, MemorySegment.NULL);
            int status = RingloomNative.tailerOpen(
                nativeHandle,
                startIndex,
                outTailer
            );
            RingloomNative.throwForStatus("ringloom_tailer_open", status);
            return new RingloomTailer(
                this,
                outTailer.get(RingloomNative.ADDRESS, 0)
            );
        }
    }

    /**
     * Drives bounded queue maintenance work across prefetcher and cleaner helpers.
     *
     * @param maxWorkUnits maximum amount of maintenance work to attempt
     * @return summary of whether work was available or completed
     */
    public StepResult maintenancePoll(int maxWorkUnits) {
        ensureOpen();
        return pollStep(
            "ringloom_queue_maintenance_poll",
            maxWorkUnits,
            RingloomNative::queueMaintenancePoll
        );
    }

    /**
     * Drives bounded queue-side prefetch work.
     *
     * <p>In ABI v1 this is an alias of queue maintenance polling.</p>
     *
     * @param maxWorkUnits maximum amount of work to attempt
     * @return summary of whether work was available or completed
     */
    public StepResult prefetchPoll(int maxWorkUnits) {
        ensureOpen();
        return pollStep(
            "ringloom_queue_prefetch_poll",
            maxWorkUnits,
            RingloomNative::queuePrefetchPoll
        );
    }

    /**
     * Drives bounded queue cleaner work.
     *
     * <p>In ABI v1 this is an alias of queue maintenance polling.</p>
     *
     * @param maxWorkUnits maximum amount of work to attempt
     * @return summary of whether work was available or completed
     */
    public StepResult cleanerPoll(int maxWorkUnits) {
        ensureOpen();
        return pollStep(
            "ringloom_queue_cleaner_poll",
            maxWorkUnits,
            RingloomNative::queueCleanerPoll
        );
    }

    /**
     * Closes the native queue handle.
     *
     * <p>Applications should close appenders and tailers opened from this queue before closing the
     * queue itself.</p>
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        RingloomNative.queueClose(nativeHandle);
    }

    MemorySegment nativeHandle() {
        ensureOpen();
        return nativeHandle;
    }

    void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("RingloomQueue is closed");
        }
    }

    private StepResult pollStep(
        String action,
        int maxWorkUnits,
        StepPoller poller
    ) {
        if (maxWorkUnits < 0) {
            throw new IllegalArgumentException(
                "maxWorkUnits must be non-negative"
            );
        }
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outStep = arena.allocate(ValueLayout.JAVA_INT);
            int status = poller.poll(nativeHandle, maxWorkUnits, outStep);
            RingloomNative.throwForStatus(action, status);
            return RingloomNative.readStepResult(outStep);
        }
    }

    @FunctionalInterface
    private interface StepPoller {
        int poll(MemorySegment queue, int maxWorkUnits, MemorySegment outStep);
    }
}
