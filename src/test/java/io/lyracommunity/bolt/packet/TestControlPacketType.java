package io.lyracommunity.bolt.packet;

import org.junit.Test;

import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class TestControlPacketType {

    @Test
    public void testSequenceNumber1() {
        ControlPacket p = new DummyControlPacket();
        byte[] x = p.getHeader();
        byte highest = x[0];
        assertEquals(128, highest & 0x80);
    }

    @Test
    public void testControlPacketTypes() {
        ControlPacketType t = ControlPacketType.CONNECTION_HANDSHAKE;
        assertEquals(0, t.getTypeId());
        t = ControlPacketType.KEEP_ALIVE;
        assertEquals(1, t.getTypeId());
        t = ControlPacketType.ACK;
        assertEquals(2, t.getTypeId());
        t = ControlPacketType.NAK;
        assertEquals(3, t.getTypeId());
        t = ControlPacketType.SHUTDOWN;
        assertEquals(4, t.getTypeId());
        t = ControlPacketType.ACK2;
        assertEquals(5, t.getTypeId());
    }

    @Test
    public void testUniquePacketTypeIds() {
        final long typeCount = ControlPacketType.values().length;
        final long distinctTypeCount = Stream.of(ControlPacketType.values()).map(ControlPacketType::getTypeId).distinct().count();
        assertEquals(typeCount, distinctTypeCount);
    }

}
