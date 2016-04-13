package io.lyracommunity.bolt;

import io.lyracommunity.bolt.helper.Infra;
import io.lyracommunity.bolt.helper.TestObjects;
import io.lyracommunity.bolt.util.SeqNum;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.lyracommunity.bolt.helper.TestSupport.sleepUnchecked;
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
            // Send unreliable.
            for (int i = 0; i < sendCount; i++) c.send(TestObjects.unreliableUnordered(100));
            sleepUnchecked(20);
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
            sleepUnchecked(20);
            c.sendBlocking(TestObjects.finished());
        };

        doTest(packetLoss, 0, maxExpectedDeliveryCount, onReady);
    }

    @Test
    public void testReliableWithPacketLoss() throws Throwable {
        final int sendCount = 50;

        final Consumer<BoltClient> onReady = c -> {
            // Send reliable.
            for (int i = 0; i < sendCount; i++) c.send(TestObjects.reliableUnordered(100));
            sleepUnchecked(20);
            c.sendBlocking(TestObjects.finished());
        };

        doTest(0.1f, sendCount, sendCount, onReady);
    }

    @Test
    public void testSendingReliablePacketsWithSequenceNumberOverflow() throws Throwable {
        final int sendCount = SeqNum.MAX_SEQ_NUM_16_BIT + 1000;

        final Consumer<BoltClient> onReady = c -> {
            // Send reliable.
            for (int i = 0; i < sendCount; i++) c.send(TestObjects.reliableUnordered(1));
            c.flush();
            c.sendBlocking(TestObjects.finished());
        };

        doTest(0f, sendCount, sendCount, onReady);
    }


    private void doTest(float packetLoss, int minExpectedDeliveryCount, int maxExpectedDeliveryCount,
                        Consumer<BoltClient> onReady) throws Throwable {
        Infra.Builder builder = Infra.Builder.withServerAndClients(1)
                .onEventServer((ts, evt) -> {
                    if (evt instanceof TestObjects.BaseDataClass) {
                        if (deliveryCount.incrementAndGet() % 20 == 0) {
                            System.out.println(format("Recv {0} {1}", evt.getClass().getSimpleName(), deliveryCount.get()));
                        }
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
