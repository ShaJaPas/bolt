package io.lyracommunity.bolt;

import org.junit.Test;

/**
 * Created by omahoc9 on 3/24/16.
 */
public class ConnectivityIT
{



    @Test
    public void testServerReactionToClientDisconnect() throws Throwable {
        // Ensure client emits Disconnected event.
        // Ensure server emits PeerDisconnected event.
    }

    @Test
    public void testClientAutoReconnectFromLapseInConnectivity() throws Throwable {

    }

    @Test
    public void testClientTimesOut() throws Throwable {
        // Ensure client emits Disconnected event.
        // Ensure server emits PeerDisconnected event.
    }

    @Test
    public void testKeepAliveOnIdle() throws Throwable {

    }

    @Test
    public void testClientReactionToServerShutdown() throws Throwable {

    }

    @Test
    public void testClientCannotConnect() throws Throwable {

    }

    @Test
    public void testServerCannotBind() throws Throwable {

    }


}
