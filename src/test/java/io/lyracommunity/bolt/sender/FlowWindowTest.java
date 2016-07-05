package io.lyracommunity.bolt.sender;

import io.lyracommunity.bolt.packet.DataPacket;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class FlowWindowTest
{

    private volatile boolean read  = true;
    private volatile boolean write = true;
    private          int     N     = 100_000;
    private volatile boolean fail  = false;

    @Test
    public void testFillWindow() throws InterruptedException, TimeoutException {
        FlowWindow fw = new FlowWindow(false, 3, 128);

        assertTrue(fw.tryProduce(createPacket(1, 1), 10, TimeUnit.MILLISECONDS));
        assertTrue(fw.tryProduce(createPacket(2, 1), 10, TimeUnit.MILLISECONDS));
        assertTrue(fw.tryProduce(createPacket(3, 1), 10, TimeUnit.MILLISECONDS));
        assertTrue(fw.isFull());

        assertFalse("Window should be full", fw.tryProduce(createPacket(1, 1), 10, TimeUnit.MILLISECONDS));
        assertTrue(fw.isFull());

        assertEquals(1, fw.consumeData().getClassID());
        assertEquals(2, fw.consumeData().getClassID());
        assertEquals(3, fw.consumeData().getClassID());

        assertTrue(fw.isEmpty());
    }

    @Test
    public void ConsumedFromFullWindow_ProducerWaitingForSignal_ProducerIsNotified() throws Exception {
        FlowWindow fw = new FlowWindow(false, 3, 128);

        assertTrue(fw.tryProduce(createPacket(1, 1), 10, TimeUnit.MILLISECONDS));
        assertTrue(fw.tryProduce(createPacket(2, 1), 10, TimeUnit.MILLISECONDS));
        assertTrue(fw.tryProduce(createPacket(3, 1), 10, TimeUnit.MILLISECONDS));
        assertTrue(fw.isFull());

        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(10);
                fw.consumeData();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        assertTrue(fw.tryProduce(createPacket(1, 1), 100, TimeUnit.MILLISECONDS));

        assertEquals(2, fw.consumeData().getClassID());
        assertEquals(3, fw.consumeData().getClassID());
        assertEquals(1, fw.consumeData().getClassID());

        assertTrue(fw.isEmpty());
    }

    @Test
    public void testOverflow() throws InterruptedException, TimeoutException {
        FlowWindow fw = new FlowWindow(false, 3, 64);

        assertTrue(fw.tryProduce(createPacket(1, 1), 10, TimeUnit.MILLISECONDS));
        assertTrue(fw.tryProduce(createPacket(2, 1), 10, TimeUnit.MILLISECONDS));
        assertTrue(fw.tryProduce(createPacket(3, 1), 10, TimeUnit.MILLISECONDS));
        assertTrue(fw.isFull());

        // Read one
        assertEquals(1, fw.consumeData().getClassID());
        assertFalse(fw.isFull());

        // Now a slot for writing should be free again
        assertTrue(fw.tryProduce(createPacket(4, 1), 10, TimeUnit.MILLISECONDS));
        fw.consumeData();

        assertTrue(fw.tryProduce(createPacket(1, 1), 10, TimeUnit.MILLISECONDS));

        assertEquals(3, fw.consumeData().getClassID());
        assertEquals(4, fw.consumeData().getClassID());
        // Which is again 1
        assertEquals(1, fw.consumeData().getClassID());

    }

    @Test
    public void testConcurrentReadWrite_20() throws InterruptedException {
        final FlowWindow fw = new FlowWindow(false, 20, 64);
        CompletableFuture.runAsync(() -> doRead(fw));
        CompletableFuture.runAsync(() -> doWrite(fw));

        int c = 0;
        while (read && write && c < 500) {
            Thread.sleep(20);
            c++;
        }
        assertFalse("An error occured in reader or writer", fail);
    }

    @Test
    public void testConcurrentReadWrite_2() throws InterruptedException {
        final FlowWindow fw = new FlowWindow(false, 2, 64);

        CompletableFuture.runAsync(() -> doRead(fw));
        CompletableFuture.runAsync(() -> doWrite(fw));

        int c = 0;
        while (read && write && c < 500) {
            Thread.sleep(20);
            c++;
        }
        assertFalse("An error occurred in reader or writer", fail);
    }

    private void doRead(final FlowWindow fw) {
        System.out.println("Starting reader...");
        try {
            for (int i = 0; i < N; i++) {
                DataPacket p;
                do {
                    p = fw.consumeData();
                }
                while (p == null);
                assertEquals(i, p.getClassID());
            }
        }
        catch (Throwable ex) {
            ex.printStackTrace();
            System.out.println(fw);
            fail = true;
        }
        System.out.println("Exiting reader...");
        read = false;
    }

    private void doWrite(final FlowWindow fw) {
        System.out.println("Starting writer...");
        try {
            for (int i = 0; i < N; i++) {
                boolean complete = false;
                while (!complete) complete = fw.tryProduce(createPacket(i, 10), 10, TimeUnit.MILLISECONDS);
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("ERROR****");
            fail = true;
        }
        System.out.println("Exiting writer...");
        write = false;
    }

    private DataPacket createPacket(final int classId, final int dataSize) {
        final byte[] data = new byte[dataSize];
        new Random().nextBytes(data);

        final DataPacket p = new DataPacket();
        p.setClassID(classId);
        p.setData(data);
        return p;
    }

}
