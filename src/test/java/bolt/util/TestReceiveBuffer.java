package bolt.util;

import bolt.packets.DataPacket;
import bolt.packets.DeliveryType;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TestReceiveBuffer {

    @Test
    public void testInOrder() {
        final ReceiveBuffer b = new ReceiveBuffer(16);
        byte[] test1 = "test1".getBytes();
        byte[] test2 = "test2".getBytes();
        byte[] test3 = "test3".getBytes();

        b.offer(orderedDataPacket(1, 1, test1));
        b.offer(orderedDataPacket(2, 2, test2));
        b.offer(orderedDataPacket(3, 3, test3));

        for (int i = 0; i < 3; i++) {
            final DataPacket a = b.poll();
            assertEquals(1 + i, a.getPacketSeqNumber());
        }

        assertNull(b.poll());
    }

    @Test
    public void testOutOfOrder() {
        ReceiveBuffer b = new ReceiveBuffer(16);
        byte[] test1 = "test1".getBytes();
        byte[] test2 = "test2".getBytes();
        byte[] test3 = "test3".getBytes();

        b.offer(orderedDataPacket(3, 3, test3));
        b.offer(orderedDataPacket(2, 2, test2));
        b.offer(orderedDataPacket(1, 1, test1));


        for (int i = 0; i < 3; i++) {
            final DataPacket a = b.poll();
            assertEquals(1 + i, a.getPacketSeqNumber());
        }

        assertNull(b.poll());
    }

    @Test
    public void testInterleaved() {
        final ReceiveBuffer b = new ReceiveBuffer(16);
        byte[] test1 = "test1".getBytes();
        byte[] test2 = "test2".getBytes();
        byte[] test3 = "test3".getBytes();

        b.offer(orderedDataPacket(3, 3, test3));

        b.offer(orderedDataPacket(1, 1, test1));

        DataPacket a = b.poll();
        assertEquals(1, a.getPacketSeqNumber());

        assertNull(b.poll());

        b.offer(orderedDataPacket(2, 2, test2));

        a = b.poll();
        assertEquals(2, a.getPacketSeqNumber());

        a = b.poll();
        assertEquals(3, a.getPacketSeqNumber());
    }

    @Test
    public void testBufferOverflow() {
        ReceiveBuffer b = new ReceiveBuffer(4);

        for (int i = 0; i < 3; i++) {
            b.offer(orderedDataPacket(i + 1, i + 1, "test".getBytes()));
        }
        for (int i = 0; i < 3; i++) {
            assertEquals(i + 1, b.poll().getPacketSeqNumber());
        }

        for (int i = 0; i < 3; i++) {
            b.offer(orderedDataPacket(i + 4, i + 4, "test".getBytes()));
        }
        for (int i = 0; i < 3; i++) {
            assertEquals(i + 4, b.poll().getPacketSeqNumber());
        }
    }

    @Test
    public void testTimedPoll() throws Exception {
        final ReceiveBuffer b = new ReceiveBuffer(4);

        CompletableFuture.runAsync(() -> {
            try {
                for (int i = 0; i < 5; i++) {
                    Thread.sleep(50);
                    b.offer(orderedDataPacket(i + 1, i + 1, "test".getBytes()));
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        });

        CompletableFuture.supplyAsync(() -> {
            for (int i = 0; i < 5; i++) {
                DataPacket r = null;
                do {
                    try {
                        r = b.poll(20, TimeUnit.MILLISECONDS);
                    }
                    catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }
                }
                while (r == null);
            }
            return "OK.";
        }).join();
    }

    @Test
    public void testTimedPoll2() throws Exception {
        final ReceiveBuffer b = new ReceiveBuffer(4);

        final AtomicBoolean poll = new AtomicBoolean(false);

        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(297);
                System.out.println("PUT");
                while (!poll.get()) Thread.sleep(10);
                b.offer(orderedDataPacket(1, 1, "test".getBytes()));
                System.out.println("... PUT OK");
            }
            catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        });

        CompletableFuture.supplyAsync(() -> {
            DataPacket r = null;
            do {
                try {
                    poll.set(true);
                    System.out.println("POLL");
                    r = b.poll(100, TimeUnit.MILLISECONDS);
                    poll.set(false);
                    if (r != null) System.out.println("... POLL OK");
                    else System.out.println("... nothing.");
                }
                catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            }
            while (r == null);
            return "OK.";
        }).join();
    }

    @Test
    public void testDuplicateUnordered() {
        final ReceiveBuffer b = new ReceiveBuffer(16);
        byte[] test1 = "test1".getBytes();
        byte[] test2 = "test2".getBytes();

        b.offer(dataPacket(1, test1, DeliveryType.RELIABLE_UNORDERED));
        b.offer(dataPacket(1, test1, DeliveryType.RELIABLE_UNORDERED));
        b.offer(dataPacket(2, test2, DeliveryType.RELIABLE_UNORDERED));

        assertEquals(1, b.poll().getPacketSeqNumber());
        assertEquals(2, b.poll().getPacketSeqNumber());
        assertNull(b.poll());
    }

    @Test
    public void testDuplicateUnorderedAndOrdered() {
        final ReceiveBuffer b = new ReceiveBuffer(16);

        b.offer(dataPacket(1, "test1".getBytes(), DeliveryType.RELIABLE_UNORDERED));   // Read 1st
        b.offer(orderedDataPacket(2, 1, "test2".getBytes()));     // Read 4th
        b.offer(dataPacket(3, "test3".getBytes(), DeliveryType.UNRELIABLE_UNORDERED)); // Read 2nd
        b.offer(dataPacket(4, "test4".getBytes(), DeliveryType.RELIABLE_UNORDERED));   // Read 3rd
        b.offer(orderedDataPacket(5, 2, "test5".getBytes()));     // Read 5th

        assertEquals(1, b.poll().getPacketSeqNumber());
        assertEquals(3, b.poll().getPacketSeqNumber());
        assertEquals(4, b.poll().getPacketSeqNumber());
        assertEquals(2, b.poll().getPacketSeqNumber());
        assertEquals(5, b.poll().getPacketSeqNumber());
    }

    @Test
    public void testOrderSequenceOverflow() {
        int orderSeqNum = SeqNum.MAX_SEQ_NUM_16_BIT - 2;
        final ReceiveBuffer b = new ReceiveBuffer(16, orderSeqNum);

        for (int i = 0; i < 5; i++) {
            orderSeqNum = SeqNum.increment16(orderSeqNum);
            b.offer(orderedDataPacket(i + 1, orderSeqNum, ("test" + i).getBytes()));
            DataPacket d = b.poll();
            assertEquals(i + 1, d.getPacketSeqNumber());
            assertEquals(orderSeqNum, d.getOrderSeqNumber());
        }

        assertEquals(2, orderSeqNum);
    }

    @Test
    public void testOrderSequenceOverflow2() {
        final int initialOrderSeqNum = SeqNum.MAX_SEQ_NUM_16_BIT - 2;
        final ReceiveBuffer b = new ReceiveBuffer(16, initialOrderSeqNum);

        int orderSeqNum = initialOrderSeqNum;
        for (int i = 0; i < 5; i++) {
            orderSeqNum = SeqNum.increment16(orderSeqNum);
            b.offer(orderedDataPacket(i + 1, orderSeqNum, ("test" + i).getBytes()));
        }

        orderSeqNum = initialOrderSeqNum;
        for (int i = 0; i < 5; i++) {
            orderSeqNum = SeqNum.increment16(orderSeqNum);
            DataPacket d = b.poll();
            assertEquals(i + 1, d.getPacketSeqNumber());
            assertEquals(orderSeqNum, d.getOrderSeqNumber());
        }

        assertEquals(2, orderSeqNum);
    }

    private DataPacket dataPacket(int seqNo, byte[] data, DeliveryType deliveryType) {
        return dataPacket(seqNo, -1, data, deliveryType);
    }

    private DataPacket orderedDataPacket(int seqNo, int orderSeqNo, byte[] data) {
        return dataPacket(seqNo, orderSeqNo, data, DeliveryType.RELIABLE_ORDERED);
    }

    private DataPacket dataPacket(int seqNo, int orderSeqNo, byte[] data, DeliveryType deliveryType) {
        final DataPacket p = new DataPacket();
        p.setData(data);
        p.setPacketSeqNumber(seqNo);
        p.setDelivery(deliveryType);
        p.setOrderSeqNumber(orderSeqNo);
        return p;
    }

}
