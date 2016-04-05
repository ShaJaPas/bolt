package io.lyracommunity.bolt.performance;

import io.lyracommunity.bolt.helper.TestClient;
import io.lyracommunity.bolt.helper.TestData;
import io.lyracommunity.bolt.helper.TestPackets;
import io.lyracommunity.bolt.helper.TestServer;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

/**
 * Created by keen on 26/02/16.
 */
public class BulkPackIT
{

    private static final long PACKET_COUNT = 1_000_000;
    private static final int SIZE = 1384;

    private final Set<Throwable> errors = new HashSet<>();
    private final AtomicInteger received = new AtomicInteger(0);


    @Test
    public void testBulkPackets_randomData() throws Exception {
        doTest(true, TestData.getRandomData(SIZE));

        assertEquals(PACKET_COUNT, received.get());
    }

    @Test
    public void testBulkPackets_reliableOrdered() throws Exception {
        doTest(true, TestPackets.reliableOrdered(SIZE));

        assertEquals(PACKET_COUNT, received.get());
    }

    @Test
    public void testBulkPackets_reliableUnordered() throws Exception {
        doTest(true, TestPackets.reliableUnordered(SIZE));
//        doTest(true, TestData.getRandomData(SIZE));

        assertEquals(PACKET_COUNT, received.get());
    }

    @Test
    public void testBulkPackets_unreliable() throws Exception {
        doTest(false, TestPackets.unreliableUnordered(SIZE));
    }

    private void doTest(final boolean waitForDelivery, final Object toSend) throws Exception {
        final AtomicBoolean sendComplete = new AtomicBoolean(false);

        final TestServer srv = TestServer.runObjectServer(toSend.getClass(),
                x -> {
                    if (received.incrementAndGet() % 10_000 == 0) System.out.println("Received " + received.get());
                },
                errors::add);
        srv.server.config().setAllowSessionExpiry(false);

        System.out.println("Connect to server port " + srv.server.getPort());
        final TestClient cli = TestClient.runClient(srv.server.getPort(),
                c -> {
                    for (int i = 0; i < PACKET_COUNT; i++) {
                        c.send(toSend);
                        if (i % 10000 == 0) System.out.println(i);
                    }
                    c.flush();
                    sendComplete.set(true);
                },
                errors::add);
        cli.client.config().setAllowSessionExpiry(false);

        final Supplier<Boolean> done = waitForDelivery
                ? () -> received.get() < PACKET_COUNT
                : () -> !sendComplete.get();
        while (done.get() && errors.isEmpty()) Thread.sleep(10);
        if (!errors.isEmpty()) throw new RuntimeException(errors.iterator().next());

        cli.printStatistics().cleanup();
        srv.printStatistics().cleanup();
    }

}
