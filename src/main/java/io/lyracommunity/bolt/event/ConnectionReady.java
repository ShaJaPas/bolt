package io.lyracommunity.bolt.event;

import io.lyracommunity.bolt.BoltSession;

/**
 * Created by keen on 03/03/16.
 */
public class ConnectionReady
{

    private final BoltSession session;

    public ConnectionReady(final BoltSession session) {
        this.session = session;
    }

    public BoltSession getSession() {
        return session;
    }

}
