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
 * Tests for the various list and queue classes.
 */
public class TestList {

    @Test
    public void testCircularArray() {
        final CircularArray<Integer> c = new CircularArray<>(5);
        for (int i = 0; i < 5; i++) c.add(i);
        assertEquals(5, c.size());
        c.add(6);
        assertEquals(5, c.size());
        System.out.println(c);
        c.add(7);
        System.out.println(c);
        for (int i = 8; i < 11; i++) c.add(i);
        System.out.println(c);
        c.add(11);
        System.out.println(c);
    }

    @Test
    public void testPacketHistoryWindow() {

        PacketHistoryWindow packetHistoryWindow = new PacketHistoryWindow(16);
        long offset = 1000000;
        for (int i = 0; i < 28; i++) {
            packetHistoryWindow.add(offset + i * 5000L);
        }
        // Packets arrive every 5 ms, so packet arrival rate is 200/sec
        assertEquals(200, packetHistoryWindow.getPacketArrivalSpeed());
    }

    @Test
    public void testPacketPairWindow() {
        long[] values = {2, 4, 6};
        PacketPairWindow p = new PacketPairWindow(16);
        for (long value : values) p.add(value);

        assertEquals(4.0d, p.computeMedianTimeInterval(), 0.001d);

        long[] arrivaltimes = {12, 12, 12, 12};
        PacketPairWindow p1 = new PacketPairWindow(16);
        for (int i = 0; i < values.length; i++) {
            p1.add(arrivaltimes[i]);
        }
        assertEquals(12.0d, p1.computeMedianTimeInterval(), 0.001d);
    }

    @Test
    public void testAckHistoryWindow() {
        AckHistoryEntry ackSeqNrA = new AckHistoryEntry(0, 1, 1263465050);
        AckHistoryEntry ackSeqNrB = new AckHistoryEntry(1, 2, 1263465054);
        AckHistoryEntry ackSeqNrC = new AckHistoryEntry(2, 3, 1263465058);

        AckHistoryWindow recvWindow = new AckHistoryWindow(3);
        recvWindow.add(ackSeqNrA);
        recvWindow.add(ackSeqNrB);
        recvWindow.add(ackSeqNrC);
        AckHistoryEntry entryA = recvWindow.getEntry(1);
        assertEquals(1263465050, entryA.getSentTime());
    }

    @Test
    public void testSenderLossList1() {
        Integer A = 7;
        Integer B = 8;
        Integer C = 1;
        SenderLossList l = new SenderLossList();
        l.insert(A);
        l.insert(B);
        l.insert(C);
        assertEquals(3, l.size());
        Integer oldest = l.getFirstEntry();
        assertEquals(C, oldest);
        oldest = l.getFirstEntry();
        assertEquals(A, oldest);
        oldest = l.getFirstEntry();
        assertEquals(B, oldest);
    }

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
