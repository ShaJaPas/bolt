package io.lyracommunity.bolt.api.event;

import io.lyracommunity.bolt.api.BoltEvent;

/**
 * Event signalling that a message has been received.
 *
 * @author Cian.
 */
public class ReceiveObject<T> implements BoltEvent {

    private final long sessionID;

    private final T payload;

    public ReceiveObject(final long sessionID, final T payload) {
        this.sessionID = sessionID;
        this.payload = payload;
    }

    /**
     * @return the ID of the session that this data was routed from.
     */
    public long getSessionID() {
        return sessionID;
    }

    public T getPayload() {
        return payload;
    }

    public boolean isOfType(final Class<?> expected) {
        return payload != null && expected.equals(payload.getClass());
    }

    public boolean isOfSubType(final Class<?> expected) {
        return payload != null && expected.isAssignableFrom(payload.getClass());
    }

    @Override
    public BoltEventType getEventType() {
        return BoltEventType.RECEIVE_OBJECT;
    }

}
