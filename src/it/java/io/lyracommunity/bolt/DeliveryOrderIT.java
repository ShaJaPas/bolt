package io.lyracommunity.bolt;

import io.lyracommunity.bolt.helper.Infra;
import io.lyracommunity.bolt.helper.TestObjects;
import io.lyracommunity.bolt.helper.TestObjects.ReliableOrdered;
import io.lyracommunity.bolt.helper.TestObjects.ReliableUnordered;
import org.junit.Test;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by omahoc9 on 3/24/16.
 */
public class DeliveryOrderIT
{

    @Test
    public void sendOrderedPacketsWithPacketLoss_InOrderDelivery() throws Throwable {
        final int sendCount = 100;
        final LinkedList<ReliableOrdered> receiveOrder = new LinkedList<>();
        final AtomicInteger sendIndex = new AtomicInteger(1);

        Infra.Builder builder = Infra.Builder.withServerAndClients(1)
                .preconfigureServer(s -> s.config().setPacketLoss(0.2f))
                .onEventServer((ts, evt) -> {
                    if (ReliableOrdered.class.equals(evt.getClass())) {
                        receiveOrder.add((ReliableOrdered) evt);
                    }
                })
                .onReadyClient((tc, evt) -> {
                    for (int i = 0; i < sendCount; i++) {
                        tc.client.send(TestObjects.reliableOrdered(sendIndex.getAndIncrement()));
                    }
                })
                .setWaitCondition(ts -> ts.server().receivedOf(ReliableOrdered.class) < sendCount);

        try (Infra i = builder.build()) {
            i.start().awaitCompletion(2, TimeUnit.MINUTES);

            for (int n = 0; n < receiveOrder.size(); n++) {
                assertEquals(n + 1, receiveOrder.get(n).getData().size());
            }
        }
    }

    @Test
    public void sendUnorderedPacketsWithPacketLoss_OutOfOrderDelivery() throws Throwable {
        final int sendCount = 100;
        final LinkedList<ReliableUnordered> receiveOrder = new LinkedList<>();
        final AtomicInteger sendIndex = new AtomicInteger(1);

        Infra.Builder builder = Infra.Builder.withServerAndClients(1)
                .preconfigureServer(s -> s.config().setPacketLoss(0.2f))
                .onEventServer((ts, evt) -> {
                    if (ReliableUnordered.class.equals(evt.getClass())) {
                        receiveOrder.add((ReliableUnordered) evt);
                    }
                })
                .onReadyClient((tc, evt) -> {
                    for (int i = 0; i < sendCount; i++) {
                        tc.client.send(TestObjects.reliableUnordered(sendIndex.getAndIncrement()));
                    }
                })
                .setWaitCondition(ts -> ts.server().receivedOf(ReliableUnordered.class) < sendCount);

        try (Infra i = builder.build()) {
            i.start().awaitCompletion(2, TimeUnit.MINUTES);

            boolean allInOrder = true;
            for (int n = 0; n < receiveOrder.size(); n++) {
                allInOrder &= (n + 1 == receiveOrder.get(n).getData().size());
            }
            assertFalse(allInOrder);
        }
    }

}
