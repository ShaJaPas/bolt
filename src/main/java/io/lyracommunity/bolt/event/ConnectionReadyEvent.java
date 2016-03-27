package io.lyracommunity.bolt.event;

import io.lyracommunity.bolt.BoltSession;

/**
 * Created by keen on 03/03/16.
 */
public class ConnectionReadyEvent {

    private final BoltSession sessionReady;

    public ConnectionReadyEvent(final BoltSession sessionReady) {
        this.sessionReady = sessionReady;
    }

    public BoltSession getSessionReady() {
        return sessionReady;
    }

}
