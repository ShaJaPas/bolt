package io.lyracommunity.bolt;

import io.lyracommunity.bolt.helper.Infra;
import io.lyracommunity.bolt.helper.TestObjects;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
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

        doTest(packetLoss, 0, maxExpectedDeliveryCount, onReady);
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
                c.send(TestObjects.reliableUnordered(100));
            try {
                Thread.sleep(20L);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            c.sendBlocking(TestObjects.finished());
        };

        doTest(packetLoss, 0, maxExpectedDeliveryCount, onReady);
    }

    @Test
    public void testReliableWithPacketLoss() throws Throwable {
        // TODO implement test
    }

    @Test
    public void testSendingReliablePacketsWithSequenceNumberOverflow() {
        // TODO implement test
    }


    private void doTest(float packetLoss, int minExpectedDeliveryCount, int maxExpectedDeliveryCount,
                        Consumer<BoltClient> onReady) throws Throwable {
        Infra.InfraBuilder builder = Infra.InfraBuilder.withServerAndClients(1)
                .onEventServer((ts, evt) -> {
                    if (evt instanceof TestObjects.BaseDataClass) {
                        deliveryCount.incrementAndGet();
                        System.out.println(format("Recv {0} {1}", evt.getClass().getSimpleName(), deliveryCount.get()));
                    }
                    else if (evt instanceof TestObjects.Finished) {
                        completed.set(true);
                    }
                })
                .preconfigureServer(s -> s.config().setPacketLoss(packetLoss))
                .onReadyClient((tc, rdy) -> onReady.accept(tc.client))
                .setWaitCondition(tc -> !completed.get());

        try (Infra i = builder.build()) {
            i.start();
            i.awaitCompletion(1, TimeUnit.MINUTES);

            System.out.println(format("Received a total of [{0}] packets of min/max [{1}/{2}].",
                    deliveryCount.get(), minExpectedDeliveryCount, maxExpectedDeliveryCount));
            assertTrue(deliveryCount.get() <= maxExpectedDeliveryCount && deliveryCount.get() >= minExpectedDeliveryCount);
        }
    }

}
