// SPDX-License-Identifier: Apache-2.0

package io.ringloom.queue;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

/**
 * Borrowed view of a message returned by {@link RingloomTailer#poll(RingloomMessageView)}.
 *
 * <p>The payload segment points at memory owned by the queue mmap. It is valid until the next poll
 * on the same tailer or until that tailer is closed. Call {@link #payloadBytes()} if the bytes must
 * outlive that borrowed window.</p>
 */
public final class RingloomMessageView {

    private long index;

    /**
     * Creates an empty borrowed message view.
     */
    public RingloomMessageView() {}

    private MemorySegment payloadSegment = MemorySegment.NULL;
    private long payloadLength;

    /**
     * Returns the public queue index assigned to the current borrowed message.
     *
     * @return message index, or {@code 0} when this view is empty
     */
    public long index() {
        return index;
    }

    /**
     * Returns the borrowed native payload segment for the current message.
     *
     * @return borrowed payload segment, or {@link MemorySegment#NULL} when this view is empty
     */
    public MemorySegment payloadSegment() {
        return payloadSegment;
    }

    /**
     * Returns the number of payload bytes in the current borrowed message.
     *
     * @return payload length in bytes
     */
    public long payloadLength() {
        return payloadLength;
    }

    /**
     * Copies the borrowed payload into an owned Java byte array.
     *
     * @return copied payload bytes
     */
    public byte[] payloadBytes() {
        if (payloadLength > Integer.MAX_VALUE) {
            throw new IllegalStateException(
                "payload is too large to copy into a Java byte[]"
            );
        }
        byte[] copy = new byte[(int) payloadLength];
        if (payloadLength != 0) {
            MemorySegment.copy(
                payloadSegment,
                ValueLayout.JAVA_BYTE,
                0,
                copy,
                0,
                (int) payloadLength
            );
        }
        return copy;
    }

    /**
     * Decodes the copied payload as a UTF-8 string.
     *
     * @return payload decoded as UTF-8
     */
    public String payloadString() {
        return new String(payloadBytes(), StandardCharsets.UTF_8);
    }

    void set(long index, MemorySegment payloadSegment, long payloadLength) {
        this.index = index;
        this.payloadSegment = payloadSegment;
        this.payloadLength = payloadLength;
    }

    void clear() {
        this.index = 0L;
        this.payloadSegment = MemorySegment.NULL;
        this.payloadLength = 0L;
    }
}
