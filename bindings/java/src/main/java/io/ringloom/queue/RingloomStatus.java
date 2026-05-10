// SPDX-License-Identifier: Apache-2.0

package io.ringloom.queue;

/**
 * Stable native status codes returned by the ringloom-queue C ABI.
 */
public final class RingloomStatus {

    /** Operation completed successfully. */
    public static final int OK = 0;
    /** Non-blocking poll completed successfully but no message was available. */
    public static final int OK_NOT_READY = 1;
    /** One or more arguments were invalid. */
    public static final int INVALID_ARGUMENT = -1;
    /** Native allocation failed. */
    public static final int OUT_OF_MEMORY = -2;
    /** The configured queue directory does not exist or is not initialized. */
    public static final int QUEUE_NOT_FOUND = -3;
    /** Queue metadata or queue-file headers were invalid or corrupt. */
    public static final int METADATA_CORRUPT = -4;
    /** A required mmap operation failed. */
    public static final int MMAP_FAILED = -5;
    /** The queue already has an active appender lease. */
    public static final int APPENDER_ALREADY_OPEN = -6;
    /** The payload exceeded the queue's supported maximum entry size. */
    public static final int MESSAGE_TOO_LARGE = -7;
    /** The payload was empty when a non-empty append was required. */
    public static final int EMPTY_PAYLOAD = -8;
    /** A message payload could not be parsed by the selected codec. */
    public static final int PARSE_FAILED = -9;
    /** An unexpected internal native failure occurred. */
    public static final int INTERNAL_ERROR = -255;

    private RingloomStatus() {}

    /**
     * Returns whether a native status code represents success.
     *
     * @param status native status code to test
     * @return {@code true} when the status is {@link #OK}
     */
    public static boolean isOk(int status) {
        return status == OK;
    }

    /**
     * Returns whether a native status code represents an empty non-blocking poll.
     *
     * @param status native status code to test
     * @return {@code true} when the status is {@link #OK_NOT_READY}
     */
    public static boolean isNotReady(int status) {
        return status == OK_NOT_READY;
    }
}
