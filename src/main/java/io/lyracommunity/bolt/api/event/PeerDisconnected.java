package io.lyracommunity.bolt.api.event;

import io.lyracommunity.bolt.api.BoltEvent;

/**
 * Event signalling that a peer has been disconnected.
 * <p>
 * This will be emitted by both client and server. On a client,
 * this occurs when the client disconnects (or is disconnected).
 * On a server, this occurs when an individual client is disconnected.
 *
 * @author Cian.
 */
public class PeerDisconnected implements BoltEvent {

    private final long sessionID;

    private final String reason;

    public PeerDisconnected(final long sessionID, final String reason) {
        this.sessionID = sessionID;
        this.reason = reason;
    }

    public long getSessionID() {
        return sessionID;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return "PeerDisconnected{" +
                "sessionID=" + sessionID +
                ", reason='" + reason + '\'' +
                '}';
    }

    @Override
    public BoltEventType getEventType() {
        return BoltEventType.PEER_DISCONNECTED;
    }

}
