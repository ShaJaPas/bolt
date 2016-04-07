package io.lyracommunity.bolt.event;

import io.lyracommunity.bolt.session.Session;

/**
 * Created by keen on 03/03/16.
 */
public class ConnectionReady
{

    private final Session session;

    public ConnectionReady(final Session session) {
        this.session = session;
    }

    public Session getSession() {
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
