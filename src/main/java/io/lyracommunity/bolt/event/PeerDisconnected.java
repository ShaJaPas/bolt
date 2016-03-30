package io.lyracommunity.bolt.event;

/**
 * Created by keen on 29/03/16.
 */
public class PeerDisconnected {

    private final int sessionID;

    public PeerDisconnected(final int sessionID) {
        this.sessionID = sessionID;
    }

    public int getSessionID() {
        return sessionID;
    }

}
