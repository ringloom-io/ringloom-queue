// SPDX-License-Identifier: Apache-2.0

package io.ringloom.queue;

/**
 * Owned message copy returned by {@link RingloomTailer#pollCopy()}.
 *
 * @param index public queue index assigned by the appender
 * @param payload copied message payload
 */
public record RingloomMessage(long index, byte[] payload) {}
