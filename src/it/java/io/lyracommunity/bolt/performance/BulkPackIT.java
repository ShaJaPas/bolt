package io.lyracommunity.bolt.performance;

import io.lyracommunity.bolt.helper.Infra;
import io.lyracommunity.bolt.helper.TestData;
import io.lyracommunity.bolt.helper.TestObjects;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
        doTest(true, TestObjects.reliableOrdered(SIZE));

        assertEquals(PACKET_COUNT, received.get());
    }

    @Test
    public void testBulkPackets_reliableUnordered() throws Exception {
        doTest(true, TestObjects.reliableUnordered(SIZE));
//        doTest(true, TestData.getRandomData(SIZE));

        assertEquals(PACKET_COUNT, received.get());
    }

    @Test
    public void testBulkPackets_unreliable() throws Exception {
        doTest(false, TestObjects.unreliableUnordered(SIZE));
    }

    private void doTest(final boolean waitForDelivery, final Object toSend) throws Exception {
        final AtomicBoolean sendComplete = new AtomicBoolean(false);

        Infra.InfraBuilder builder = Infra.InfraBuilder.withServerAndClients(1)
                .preconfigureServer(s -> s.config().setAllowSessionExpiry(false))
                .preconfigureClients(c -> c.config().setAllowSessionExpiry(false))
                .onEventServer((ts, evt) -> {
                    if (evt.getClass().equals(toSend.getClass()) && received.incrementAndGet() % 10_000 == 0)
                        System.out.println("Received " + received.get());
                })
                .onReadyClient((tc, evt) -> {
                    for (int i = 0; i < PACKET_COUNT; i++) {
                        tc.client.send(toSend);
                        if (i % 10000 == 0) System.out.println(i);
                    }
                    tc.client.flush();
                    sendComplete.set(true);
                })
                .setWaitCondition(ts -> waitForDelivery
                        ? ts.getTotalReceived(toSend.getClass()) < PACKET_COUNT
                        : !sendComplete.get());

        try (Infra i = builder.build()) {
            i.start().awaitCompletion(5, TimeUnit.MINUTES);
        }
    }

}
