package io.lyracommunity.bolt.event;

/**
 * Created by keen on 06/03/16.
 */
public class ReceiveObject<T>
{

    private final long sessionID;

    private final T payload;

    public ReceiveObject(final long sessionID, final T payload) {
        this.sessionID = sessionID;
        this.payload = payload;
    }

    /**
     * The ID of the session that this data was routed from.
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

}
