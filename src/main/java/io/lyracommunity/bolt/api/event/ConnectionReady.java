package io.lyracommunity.bolt.api.event;

import io.lyracommunity.bolt.api.BoltEvent;
import io.lyracommunity.bolt.session.Session;

/**
 * Event signalling that a connection has been established.
 *
 * @author Cian.
 */
public class ConnectionReady implements BoltEvent {

    private final Session session;

    public ConnectionReady(final Session session) {
        this.session = session;
    }

    public Session getSession() {
        return session;
    }

    @Override
    public String toString() {
        return "ConnectionReady{" +
                "session=" + session +
                '}';
    }

    @Override
    public BoltEventType getEventType() {
        return BoltEventType.CONNECTION_READY;
    }

}
