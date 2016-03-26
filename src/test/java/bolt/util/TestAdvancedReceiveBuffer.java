package bolt.util;

import bolt.packets.DataPacket;
import bolt.packets.DeliveryType;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TestAdvancedReceiveBuffer {

    private DataPacket dataPacket(int seqNo, byte[] data) {
        return dataPacket(seqNo, data, DeliveryType.RELIABLE_ORDERED);
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

    @Test
    public void testInOrder() {
        final AdvancedReceiveBuffer b = new AdvancedReceiveBuffer(16, 1);
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
        AdvancedReceiveBuffer b = new AdvancedReceiveBuffer(16, 1);
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
        final AdvancedReceiveBuffer b = new AdvancedReceiveBuffer(16, 1);
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
    public void testOverflow() {
        AdvancedReceiveBuffer b = new AdvancedReceiveBuffer(4, 1);

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
        final AdvancedReceiveBuffer b = new AdvancedReceiveBuffer(4, 1);

        final Runnable write = () -> {
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
        };

        final Callable<String> reader = () -> {
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
        };

        ScheduledExecutorService es = Executors.newScheduledThreadPool(2);
        es.execute(write);
        Future<String> res = es.submit(reader);
        res.get();
        es.shutdownNow();
    }

    @Test
    public void testTimedPoll2() throws Exception {
        final AdvancedReceiveBuffer b = new AdvancedReceiveBuffer(4, 1);

        final AtomicBoolean poll = new AtomicBoolean(false);

        Runnable write = () -> {
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
        };

        Callable<String> reader = () -> {
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
        };

        ScheduledExecutorService es = Executors.newScheduledThreadPool(2);
        es.execute(write);
        Future<String> res = es.submit(reader);
        res.get();
        es.shutdownNow();
    }

    @Test
    public void testDuplicateUnordered() {
        final AdvancedReceiveBuffer b = new AdvancedReceiveBuffer(16, 1);
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
        final AdvancedReceiveBuffer b = new AdvancedReceiveBuffer(16, 1);

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

}
