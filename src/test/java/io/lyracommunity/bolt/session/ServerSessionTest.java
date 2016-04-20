package io.lyracommunity.bolt.session;

import io.lyracommunity.bolt.ChannelOutStub;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.helper.PortUtil;
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

    private ChannelOutStub endpoint;

    @Before
    public void setUp() throws Exception
    {
        remote = new Destination(InetAddress.getLocalHost(), PortUtil.nextClientPort());
        final Config config = new Config(InetAddress.getLocalHost(), PortUtil.nextServerPort());
        endpoint = new ChannelOutStub(config, true);
        sut = new ServerSession(config, endpoint, remote);
    }

    @Test
    public void receiveHandshakesCorrectly_SessionEstablished() throws Exception
    {
        boolean established = sut.receiveHandshake(null, ConnectionHandshake.ofClientInitial(10, 1, 1000, 1,
                InetAddress.getLocalHost()), remote);
        assertFalse(established);

        established = sut.receiveHandshake(null, ConnectionHandshake.ofClientSecond(10, 1, 1000, 1, sut.getSessionID(),
                sut.getState().getSessionCookie(), InetAddress.getLocalHost()), remote);
        assertTrue(established);
    }

    @Test
    public void receiveHandshakesCorrectly_BadCookie() throws Exception
    {
        boolean established = sut.receiveHandshake(null, ConnectionHandshake.ofClientInitial(10, 1, 1000, 1,
                InetAddress.getLocalHost()), remote);
        assertFalse(established);

        established = sut.receiveHandshake(new TestSubscriber<>(), ConnectionHandshake.ofClientSecond(10, 1, 1000, 1, sut.getSessionID(),
                sut.getState().getSessionCookie() - 1, InetAddress.getLocalHost()), remote);
        assertFalse(established);
    }

}