// SPDX-License-Identifier: Apache-2.0

package io.ringloom.queue;

/**
 * Runtime exception raised for non-recoverable ringloom-queue native errors.
 */
public final class RingloomQueueException extends RuntimeException {

    /** Stable native status code carried by this exception. */
    private final int status;
    /** Human-readable native status label carried by this exception. */
    private final String statusName;
    /** Native action name associated with this exception. */
    private final String action;

    RingloomQueueException(int status, String statusName, String action) {
        super(action + " failed: " + statusName + " (" + status + ")");
        this.status = status;
        this.statusName = statusName;
        this.action = action;
    }

    /**
     * Returns the stable native status code associated with this failure.
     *
     * @return native status code
     */
    public int status() {
        return status;
    }

    /**
     * Returns the human-readable native status label.
     *
     * @return native status name
     */
    public String statusName() {
        return statusName;
    }

    /**
     * Returns the native action that failed.
     *
     * @return failing native action name
     */
    public String action() {
        return action;
    }
}
