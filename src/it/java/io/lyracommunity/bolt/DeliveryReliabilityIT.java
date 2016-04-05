package io.lyracommunity.bolt;

import io.lyracommunity.bolt.helper.TestClient;
import io.lyracommunity.bolt.helper.TestObjects;
import io.lyracommunity.bolt.helper.TestServer;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.text.MessageFormat.format;
import static org.junit.Assert.assertTrue;

/**
 * Created by keen on 15/03/16.
 */
public class DeliveryReliabilityIT
{


    private final AtomicInteger deliveryCount = new AtomicInteger(0);
    private final Set<Throwable> errors = new HashSet<>();
    private final AtomicBoolean completed = new AtomicBoolean(false);

    @Test
    public void testUnreliableWithPacketLoss() throws Throwable {

        final float packetLoss = 0.1f;
        final int sendCount = 50;
        final int maxExpectedDeliveryCount = (int) Math.ceil(sendCount * (1f - packetLoss));
        final Consumer<BoltClient> onReady = c -> {

            // Send unreliable
            for (int i = 0; i < sendCount; i++) c.send(TestObjects.unreliableUnordered(100));
            try {
                Thread.sleep(20L);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            c.sendBlocking(TestObjects.finished());
        };

        startTest(packetLoss, 0, maxExpectedDeliveryCount, onReady);
    }


    @Test
    public void testUnreliableAndReliableWithPacketLoss() throws Throwable {
        final float packetLoss = 0.1f;
        final int sendCount = 50;
        final int maxExpectedDeliveryCount = (int) Math.ceil(sendCount * (1f - packetLoss)) + (sendCount); // (unreliable) + (reliable)
        final Consumer<BoltClient> onReady = c -> {

            // Send unreliable
            for (int i = 0; i < sendCount; i++) c.send(TestObjects.unreliableUnordered(100));
            for (int i = 0; i < sendCount; i++)
                c.send(TestObjects.reliableUnordered(100)); // TODO unordered is sending more packets than expected
            try {
                Thread.sleep(20L);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            c.sendBlocking(TestObjects.finished());
        };

        startTest(packetLoss, 0, maxExpectedDeliveryCount, onReady);
    }

    @Test
    public void testReliableWithPacketLoss() throws Throwable {

    }

    @Test
    public void testSendingReliablePacketsWithSequenceNumberOverflow() {

    }

    private void startTest(float packetLoss, int minExpectedDeliveryCount, int maxExpectedDeliveryCount, Consumer<BoltClient> onReady) throws Throwable {
        final TestServer srv = TestServer.runObjectServer(Object.class,
                x -> {
                    if (x.getPayload() instanceof TestObjects.BaseDataClass) {
                        deliveryCount.incrementAndGet();
                        System.out.println(format("Recv {0} {1}", x.getClass().getSimpleName(), deliveryCount.get()));
                    }
                    else if (x.getPayload() instanceof TestObjects.Finished) {
                        completed.set(true);
                    }
                },
                errors::add);
        srv.server.config().setPacketLoss(packetLoss);

        TestClient cli = TestClient.runClient(srv.server.getPort(), onReady::accept, errors::add);

        while (!completed.get() && errors.isEmpty()) {
            if (!errors.isEmpty()) throw errors.iterator().next();
            Thread.sleep(10);
        }

        System.out.println(format("Received a total of [{0}] packets of min/max [{1}/{2}].",
                deliveryCount.get(), minExpectedDeliveryCount, maxExpectedDeliveryCount));
        assertTrue(deliveryCount.get() <= maxExpectedDeliveryCount && deliveryCount.get() >= minExpectedDeliveryCount);

        srv.cleanup();
        cli.cleanup();
    }

}
