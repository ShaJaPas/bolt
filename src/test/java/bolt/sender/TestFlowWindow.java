package bolt.sender;

import bolt.packets.DataPacket;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class TestFlowWindow {

    volatile boolean read = true;
    volatile boolean write = true;
    int N = 100000;
    private volatile boolean fail = false;

    @Test
    public void testFillWindow() throws InterruptedException, TimeoutException {
        FlowWindow fw = new FlowWindow(3, 128);

        assertTrue(fw.tryProduce(p -> p.setClassID(1)));
        assertTrue(fw.tryProduce(p -> p.setClassID(2)));
        assertTrue(fw.tryProduce(p -> p.setClassID(3)));
        assertTrue(fw.isFull());

        assertFalse("Window should be full", fw.tryProduce(p -> p.setClassID(4)));
        assertTrue(fw.isFull());

        assertEquals(1, fw.consumeData().getClassID());
        assertEquals(2, fw.consumeData().getClassID());
        assertEquals(3, fw.consumeData().getClassID());

        assertTrue(fw.isEmpty());
    }

    @Test
    public void testOverflow() throws InterruptedException, TimeoutException {
        FlowWindow fw = new FlowWindow(3, 64);

        assertTrue(fw.tryProduce(p -> p.setClassID(1)));
        assertTrue(fw.tryProduce(p -> p.setClassID(2)));
        assertTrue(fw.tryProduce(p -> p.setClassID(3)));
        assertTrue(fw.isFull());

        // Read one
        assertEquals(1, fw.consumeData().getClassID());
        assertFalse(fw.isFull());

        // Now a slot for writing should be free again
        assertTrue(fw.tryProduce(p -> p.setClassID(4)));
        fw.consumeData();

        assertTrue(fw.tryProduce(p -> {}));

        assertEquals(3, fw.consumeData().getClassID());
        assertEquals(4, fw.consumeData().getClassID());
        // Which is again 1
        assertEquals(1, fw.consumeData().getClassID());

    }

    @Test
    public void testConcurrentReadWrite_20() throws InterruptedException {
        final FlowWindow fw = new FlowWindow(20, 64);
        Thread reader = new Thread(() -> {
            doRead(fw);
        });
        reader.setName("reader");
        Thread writer = new Thread(() -> {
            doWrite(fw);
        });
        writer.setName("writer");

        writer.start();
        reader.start();

        int c = 0;
        while (read && write && c < 10) {
            Thread.sleep(1000);
            c++;
        }
        assertFalse("An error occured in reader or writer", fail);
    }

    @Test
    public void testConcurrentReadWrite_2() throws InterruptedException {
        final FlowWindow fw = new FlowWindow(2, 64);

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
                while ((p = fw.consumeData()) == null) {
                    Thread.sleep(1);
                }
                assertEquals(i, p.getMessageId());
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
                final int idx = i;
                boolean complete = false;
                while (!complete) complete = fw.tryProduce(p -> {
                    p.setData(("test" + idx).getBytes());
                    p.setMessageId(idx);
                });
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

}
