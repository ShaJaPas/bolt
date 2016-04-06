package io.lyracommunity.bolt.packet;

import org.junit.Test;

import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class TestControlPacketType {

    @Test
    public void testSequenceNumber1() {
        ControlPacket p = new DummyControlPacket();
        final byte[] x = p.getHeader();
        final byte highest = x[0];
        assertEquals(128, highest & 0x80);
    }

    @Test
    public void testControlPacketTypes() {
        PacketType t = PacketType.HANDSHAKE;
        assertEquals(0, t.getTypeId());
        t = PacketType.KEEP_ALIVE;
        assertEquals(1, t.getTypeId());
        t = PacketType.ACK;
        assertEquals(2, t.getTypeId());
        t = PacketType.NAK;
        assertEquals(3, t.getTypeId());
        t = PacketType.SHUTDOWN;
        assertEquals(4, t.getTypeId());
        t = PacketType.ACK2;
        assertEquals(5, t.getTypeId());
    }

    @Test
    public void testUniquePacketTypeIds() {
        final long typeCount = PacketType.values().length;
        final long distinctTypeCount = Stream.of(PacketType.values()).map(PacketType::getTypeId).distinct().count();
        assertEquals(typeCount, distinctTypeCount);
    }

}
