package bolt.packets;

import org.junit.Test;
import bolt.packets.ControlPacket.ControlPacketType;

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
        t = ControlPacketType.MESSAGE_DROP_REQUEST;
        assertEquals(6, t.getTypeId());
        t = ControlPacketType.USER_DEFINED;
        assertEquals(7, t.getTypeId());
    }
}
