package bolt;

import bolt.packets.DataPacket;
import bolt.packets.KeepAlive;
import bolt.receiver.AckHistoryEntry;
import bolt.receiver.AckHistoryWindow;
import bolt.receiver.PacketHistoryWindow;
import bolt.receiver.PacketPairWindow;
import bolt.sender.SenderLossList;
import bolt.util.CircularArray;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import static org.junit.Assert.*;

/**
 * Tests for the various list and queue classes
 */
public class TestList {


    // TODO is this class/test longer needed?
    @Test
    public void testReceiverInputQueue() {
        BlockingQueue<BoltPacket> q = new PriorityBlockingQueue<BoltPacket>(5);
        BoltPacket control = new KeepAlive();
        DataPacket d1 = new DataPacket();
        d1.setPacketSequenceNumber(1);
        DataPacket d2 = new DataPacket();
        d2.setPacketSequenceNumber(2);
        DataPacket d3 = new DataPacket();
        d3.setPacketSequenceNumber(3);
        q.offer(d3);
        q.offer(d2);
        q.offer(d1);
        q.offer(control);

        BoltPacket p1 = q.poll();
        assertTrue(p1.isControlPacket());

        BoltPacket p = q.poll();
        assertFalse(p.isControlPacket());
        // Check ordering by sequence number
        assertEquals(1, p.getPacketSequenceNumber());

        DataPacket d = new DataPacket();
        d.setPacketSequenceNumber(54);
        q.offer(d);

        p = q.poll();
        assertFalse(p.isControlPacket());
        assertEquals(2, p.getPacketSequenceNumber());

        p = q.poll();
        assertFalse(p.isControlPacket());
        assertEquals(3, p.getPacketSequenceNumber());

        p = q.poll();
        assertFalse(p.isControlPacket());
        assertEquals(54, p.getPacketSequenceNumber());

    }

}
