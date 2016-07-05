package io.lyracommunity.bolt.packet;

import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.*;

/**
 * Created by keen on 05/07/16.
 */
public class ConnectionHandshakeTest {

    private ConnectionHandshake h;

    @Before
    public void setUp() throws Exception {
        h = new ConnectionHandshake(1400, 12, 1, 40L, 256L, 1230, 4321, 1251251L, InetAddress.getLocalHost());
    }

    @Test
    public void encodeAndDecode() throws Exception {
        final byte[] encoded = h.getEncoded();

        final ConnectionHandshake h2 = (ConnectionHandshake) PacketFactory.createPacket(encoded);

        assertEquals(h, h2);

    }
}