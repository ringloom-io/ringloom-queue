// SPDX-License-Identifier: Apache-2.0

package io.ringloom.queue;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Configuration used to open or create a ringloom-queue directory.
 *
 * @param dir queue directory containing {@code metadata.ringloom} and cycle files
 * @param create whether a missing queue should be created
 * @param rollSchemeName optional built-in roll scheme name, such as {@code FAST_DAILY}
 * @param enablePrefetcher whether queue prefetch helper state should be enabled
 * @param enableCleaner whether queue cleaner helper state should be enabled
 * @param spawnHelperThreads compatibility flag recorded in the native queue options; the v1 C ABI
 *     does not spawn library-owned threads
 * @param retentionCycles optional number of recent cycles to retain; {@code null} keeps all cycles
 */
public record QueueConfig(
    String dir,
    boolean create,
    String rollSchemeName,
    boolean enablePrefetcher,
    boolean enableCleaner,
    boolean spawnHelperThreads,
    Integer retentionCycles
) {
    /**
     * Default roll scheme used by queue creation when no explicit scheme is configured.
     */
    public static final String DEFAULT_ROLL_SCHEME_NAME = "FAST_DAILY";

    /**
     * Validates the provided configuration values.
     */
    public QueueConfig {
        dir = Objects.requireNonNull(dir, "dir");
        if (dir.isBlank()) {
            throw new IllegalArgumentException("dir must not be blank");
        }
        if (rollSchemeName != null && rollSchemeName.isBlank()) {
            rollSchemeName = null;
        }
        if (retentionCycles != null && retentionCycles < 0) {
            throw new IllegalArgumentException(
                "retentionCycles must be non-negative"
            );
        }
    }

    /**
     * Creates a configuration for opening or creating a queue directory with creation enabled.
     *
     * @param dir queue directory path
     * @return configuration populated with default helper settings
     */
    public static QueueConfig create(String dir) {
        return new QueueConfig(dir, true, null, true, true, false, null);
    }

    /**
     * Creates a configuration for opening or creating a queue directory with creation enabled.
     *
     * @param dir queue directory path
     * @return configuration populated with default helper settings
     */
    public static QueueConfig create(Path dir) {
        return create(dir.toString());
    }

    /**
     * Creates a configuration for opening an existing queue directory.
     *
     * @param dir queue directory path
     * @return configuration populated with default helper settings and creation disabled
     */
    public static QueueConfig open(String dir) {
        return new QueueConfig(dir, false, null, true, true, false, null);
    }

    /**
     * Creates a configuration for opening an existing queue directory.
     *
     * @param dir queue directory path
     * @return configuration populated with default helper settings and creation disabled
     */
    public static QueueConfig open(Path dir) {
        return open(dir.toString());
    }

    /**
     * Returns a copy of this configuration with the queue creation flag updated.
     *
     * @param create whether a missing queue should be created
     * @return copied configuration with the new creation flag
     */
    public QueueConfig withCreate(boolean create) {
        return new QueueConfig(
            dir,
            create,
            rollSchemeName,
            enablePrefetcher,
            enableCleaner,
            spawnHelperThreads,
            retentionCycles
        );
    }

    /**
     * Returns a copy of this configuration with a different roll scheme name.
     *
     * @param rollSchemeName built-in roll scheme name, or {@code null} to use the queue default
     * @return copied configuration with the new roll scheme name
     */
    public QueueConfig withRollSchemeName(String rollSchemeName) {
        return new QueueConfig(
            dir,
            create,
            rollSchemeName,
            enablePrefetcher,
            enableCleaner,
            spawnHelperThreads,
            retentionCycles
        );
    }

    /**
     * Returns a copy of this configuration with queue prefetch helper state enabled or disabled.
     *
     * @param enabled whether prefetch helper state should be enabled
     * @return copied configuration with the new prefetch setting
     */
    public QueueConfig withPrefetcher(boolean enabled) {
        return new QueueConfig(
            dir,
            create,
            rollSchemeName,
            enabled,
            enableCleaner,
            spawnHelperThreads,
            retentionCycles
        );
    }

    /**
     * Returns a copy of this configuration with cleaner helper state enabled or disabled.
     *
     * @param enabled whether cleaner helper state should be enabled
     * @return copied configuration with the new cleaner setting
     */
    public QueueConfig withCleaner(boolean enabled) {
        return new QueueConfig(
            dir,
            create,
            rollSchemeName,
            enablePrefetcher,
            enabled,
            spawnHelperThreads,
            retentionCycles
        );
    }

    /**
     * Returns a copy of this configuration with the compatibility helper-thread flag updated.
     *
     * @param enabled whether native helper threads are allowed by configuration
     * @return copied configuration with the new helper-thread flag
     */
    public QueueConfig withHelperThreads(boolean enabled) {
        return new QueueConfig(
            dir,
            create,
            rollSchemeName,
            enablePrefetcher,
            enableCleaner,
            enabled,
            retentionCycles
        );
    }

    /**
     * Returns a copy of this configuration with a different retention policy.
     *
     * @param retentionCycles number of recent cycles to keep, or {@code null} to keep all cycles
     * @return copied configuration with the new retention policy
     */
    public QueueConfig withRetentionCycles(Integer retentionCycles) {
        return new QueueConfig(
            dir,
            create,
            rollSchemeName,
            enablePrefetcher,
            enableCleaner,
            spawnHelperThreads,
            retentionCycles
        );
    }
}
