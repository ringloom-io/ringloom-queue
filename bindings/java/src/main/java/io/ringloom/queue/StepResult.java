// SPDX-License-Identifier: Apache-2.0

package io.ringloom.queue;

/**
 * Result of a bounded queue or tailer maintenance poll.
 */
public enum StepResult {
    /** No work was available. */
    IDLE(0),
    /** At least one unit of work completed. */
    MADE_PROGRESS(1),
    /** More work is immediately available. */
    MORE_WORK(2);

    private final int nativeValue;

    StepResult(int nativeValue) {
        this.nativeValue = nativeValue;
    }

    /**
     * Returns the raw native integer value for this step result.
     *
     * @return native step-result value
     */
    public int nativeValue() {
        return nativeValue;
    }

    static StepResult fromNative(int value) {
        return switch (value) {
            case 0 -> IDLE;
            case 1 -> MADE_PROGRESS;
            case 2 -> MORE_WORK;
            default -> throw new IllegalArgumentException(
                "Unknown native StepResult value: " + value
            );
        };
    }
}
