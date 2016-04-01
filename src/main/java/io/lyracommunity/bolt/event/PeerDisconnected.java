package io.lyracommunity.bolt.event;

/**
 * Created by keen on 29/03/16.
 */
public class PeerDisconnected {

    private final long sessionID;

    private final String reason;

    public PeerDisconnected(final long sessionID, final String reason) {
        this.sessionID = sessionID;
        this.reason = reason;
    }

    public long getSessionID() {
        return sessionID;
    }

    public String getReason()
    {
        return reason;
    }

    @Override
    public String toString()
    {
        return "PeerDisconnected{" +
                "sessionID=" + sessionID +
                ", reason='" + reason + '\'' +
                '}';
    }

}
