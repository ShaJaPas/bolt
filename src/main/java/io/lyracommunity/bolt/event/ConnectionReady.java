package io.lyracommunity.bolt.event;

import io.lyracommunity.bolt.session.BoltSession;

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

    @Override
    public String toString()
    {
        return "ConnectionReady{" +
                "session=" + session +
                '}';
    }

}
