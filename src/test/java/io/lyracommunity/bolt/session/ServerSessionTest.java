package io.lyracommunity.bolt.session;

import io.lyracommunity.bolt.Endpoint;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.packet.ConnectionHandshake;
import io.lyracommunity.bolt.packet.Destination;
import org.junit.Before;
import org.junit.Test;
import rx.observers.TestSubscriber;

import java.net.InetAddress;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by omahoc9 on 4/5/16.
 */
public class ServerSessionTest
{

    private ServerSession sut;

    private Destination remote;

    @Before
    public void setUp() throws Exception
    {
        remote = new Destination(InetAddress.getLocalHost(), 12345);
        final Config config = new Config(InetAddress.getLocalHost(), 65432);
        sut = new ServerSession(config, new Endpoint("", config), remote);
    }

    @Test
    public void receiveHandshakesCorrectly_SessionEstablished() throws Exception
    {
        boolean established = sut.receiveHandshake(null, ConnectionHandshake.ofClientInitial(10, 1, 1000, 1,
                InetAddress.getLocalHost()), remote);
        assertFalse(established);

        established = sut.receiveHandshake(null, ConnectionHandshake.ofClientSecond(10, 1, 1000, 1, sut.getSocketID(),
                sut.getState().getSessionCookie(), InetAddress.getLocalHost()), remote);
        assertTrue(established);
    }

    @Test
    public void receiveHandshakesCorrectly_BadCookie() throws Exception
    {
        boolean established = sut.receiveHandshake(null, ConnectionHandshake.ofClientInitial(10, 1, 1000, 1,
                InetAddress.getLocalHost()), remote);
        assertFalse(established);

        established = sut.receiveHandshake(new TestSubscriber<>(), ConnectionHandshake.ofClientSecond(10, 1, 1000, 1, sut.getSocketID(),
                sut.getState().getSessionCookie() - 1, InetAddress.getLocalHost()), remote);
        assertFalse(established);
    }

}