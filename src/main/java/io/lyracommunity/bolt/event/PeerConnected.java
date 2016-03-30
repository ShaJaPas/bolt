package io.lyracommunity.bolt.event;

/**
 * Created by keen on 29/03/16.
 */
public class PeerConnected {

    private final int sessionID;

    public PeerConnected(final int sessionID) {
        this.sessionID = sessionID;
    }

    public int getSessionID() {
        return sessionID;
    }

}
