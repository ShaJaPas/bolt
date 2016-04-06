package io.lyracommunity.bolt;

import org.junit.Test;

/**
 * Created by omahoc9 on 3/24/16.
 */
public class ConnectivityIT
{



    @Test
    public void testServerReactionToClientDisconnect() throws Throwable {
        // TODO implement test
        // Ensure client emits Disconnected event.
        // Ensure server emits PeerDisconnected event.
    }

    @Test
    public void testClientAutoReconnectFromLapseInConnectivity() throws Throwable {
        // TODO implement test
    }

    @Test
    public void testClientTimesOut() throws Throwable {
        // TODO implement test
        // Ensure client emits Disconnected event.
        // Ensure server emits PeerDisconnected event.
    }

    @Test
    public void testKeepAliveOnIdle() throws Throwable {
        // TODO implement test
    }

    @Test
    public void testClientReactionToServerShutdown() throws Throwable {
        // TODO implement test
    }

    @Test
    public void testClientCannotConnect() throws Throwable {
        // TODO implement test
    }

    @Test
    public void testServerCannotBind() throws Throwable {
        // TODO implement test
    }


}
