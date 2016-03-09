package bolt.util;

import bolt.packets.DataPacket;
import org.junit.Test;

import java.util.concurrent.*;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TestReceiveBuffer {

    volatile boolean poll = false;

    private DataPacket sequencedDataPacket(int seqNo, byte[] data) {
        final DataPacket p = new DataPacket();
        p.setData(data);
        p.setPacketSequenceNumber(seqNo);
        return p;
    }

    @Test
    public void testInOrder() {
        final ReceiveBuffer b = new ReceiveBuffer(16, 1);
        byte[] test1 = "test1".getBytes();
        byte[] test2 = "test2".getBytes();
        byte[] test3 = "test3".getBytes();

        b.offer(sequencedDataPacket(1, test1));
        b.offer(sequencedDataPacket(2, test2));
        b.offer(sequencedDataPacket(3, test3));

        for (int i = 0; i < 3; i++) {
            final DataPacket a = b.poll();
            assertEquals(1 + i, a.getPacketSequenceNumber());
        }

        assertNull(b.poll());
    }

    @Test
    public void testOutOfOrder() {
        ReceiveBuffer b = new ReceiveBuffer(16, 1);
        byte[] test1 = "test1".getBytes();
        byte[] test2 = "test2".getBytes();
        byte[] test3 = "test3".getBytes();

        b.offer(sequencedDataPacket(3, test3));
        b.offer(sequencedDataPacket(2, test2));
        b.offer(sequencedDataPacket(1, test1));


        for (int i = 0; i < 3; i++) {
            final DataPacket a = b.poll();
            assertEquals(1 + i, a.getPacketSequenceNumber());
        }

        assertNull(b.poll());
    }

    @Test
    public void testInterleaved() {
        final ReceiveBuffer b = new ReceiveBuffer(16, 1);
        byte[] test1 = "test1".getBytes();
        byte[] test2 = "test2".getBytes();
        byte[] test3 = "test3".getBytes();

        b.offer(sequencedDataPacket(3, test3));

        b.offer(sequencedDataPacket(1, test1));

        DataPacket a = b.poll();
        assertEquals(1, a.getPacketSequenceNumber());

        assertNull(b.poll());

        b.offer(sequencedDataPacket(2, test2));

        a = b.poll();
        assertEquals(2, a.getPacketSequenceNumber());

        a = b.poll();
        assertEquals(3, a.getPacketSequenceNumber());
    }

    @Test
    public void testOverflow() {
        ReceiveBuffer b = new ReceiveBuffer(4, 1);

        for (int i = 0; i < 3; i++) {
            b.offer(sequencedDataPacket(i + 1, "test".getBytes()));
        }
        for (int i = 0; i < 3; i++) {
            assertEquals(i + 1, b.poll().getPacketSequenceNumber());
        }

        for (int i = 0; i < 3; i++) {
            b.offer(sequencedDataPacket(i + 4, "test".getBytes()));
        }
        for (int i = 0; i < 3; i++) {
            assertEquals(i + 4, b.poll().getPacketSequenceNumber());
        }
    }

    @Test
    public void testTimedPoll() throws Exception {
        final ReceiveBuffer b = new ReceiveBuffer(4, 1);

        final Runnable write = () -> {
            try {
                for (int i = 0; i < 5; i++) {
                    Thread.sleep(50);
                    b.offer(sequencedDataPacket(i + 1, "test".getBytes()));
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
        final ReceiveBuffer b = new ReceiveBuffer(4, 1);

        Runnable write = () -> {
            try {
                Thread.sleep(297);
                System.out.println("PUT");
                while (!poll) Thread.sleep(10);
                b.offer(sequencedDataPacket(1, "test".getBytes()));
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
                    poll = true;
                    System.out.println("POLL");
                    r = b.poll(100, TimeUnit.MILLISECONDS);
                    poll = false;
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
}
