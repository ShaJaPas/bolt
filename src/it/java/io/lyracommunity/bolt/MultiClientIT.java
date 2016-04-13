package io.lyracommunity.bolt;

import io.lyracommunity.bolt.event.PeerDisconnected;
import io.lyracommunity.bolt.event.ReceiveObject;
import io.lyracommunity.bolt.helper.Infra;
import io.lyracommunity.bolt.helper.TestClient;
import io.lyracommunity.bolt.helper.TestObjects;
import io.lyracommunity.bolt.helper.TestServer;
import org.junit.Test;
import rx.functions.Action1;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by keen on 24/03/16.
 */
public class MultiClientIT {

    private final AtomicInteger received = new AtomicInteger(0);

    @Test
    public void testMultiClientDisconnect() throws Throwable {
        final int numClients = 2 + new Random().nextInt(3);
        final AtomicInteger serverDisconnectedEvents = new AtomicInteger(0);
        final CountDownLatch awaitingConnectionReady = new CountDownLatch(numClients);

        Infra.InfraBuilder builder = Infra.InfraBuilder.withServerAndClients(numClients)
                .onEventServer((ts, evt) -> {
                    System.out.println(evt);
                    if (evt instanceof PeerDisconnected) {
                        serverDisconnectedEvents.incrementAndGet();
                    }
                })
                .onReadyClient((tc, rdy) -> {
                    awaitingConnectionReady.countDown();
                    try {
                        tc.close();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .preconfigureClients(client -> client.config().setAllowSessionExpiry(false))
                .setWaitCondition(tc -> serverDisconnectedEvents.get() < numClients);

        try (Infra i = builder.build()) {
            final long millisTaken = i.start().awaitCompletion(1, TimeUnit.MINUTES);
            System.out.println("Receive took " + millisTaken + " ms.");

            assertEquals(numClients, serverDisconnectedEvents.get());
        }
    }

    @Test
    public void testReceiveFromMultipleClients() throws Throwable {
        final TestObjects.ReliableUnordered toSend = TestObjects.reliableUnordered(100);
        final int packetCount = 500;
        final int clientCount = 4;
        final boolean sessionExpirable = false;
//        final int clientCount = 2 + new Random().nextInt(4);
        System.out.println("Total of " + clientCount + " clients.");
        final CountDownLatch clientsComplete = new CountDownLatch(clientCount);

        Infra.InfraBuilder builder = Infra.InfraBuilder.withServerAndClients(clientCount)
                .preconfigureServer(s -> s.config().setAllowSessionExpiry(sessionExpirable))
                .onEventServer((ts, evt) -> {
                    if (ReceiveObject.class.equals(evt.getClass())) {
                        ReceiveObject<?> ro = (ReceiveObject) evt;
                        if (ro.isOfSubType(toSend.getClass()) && received.incrementAndGet() % 50 == 0) {
                            System.out.println(MessageFormat.format("Received from [{0}], total {1}.", ro.getSessionID(), received.get()));
                        }
                    }
                })
                .onReadyClient((tc, rdy) -> {
                    for (int i = 0; i < packetCount; i++) tc.client.send(toSend);
                    tc.client.flush();
                    clientsComplete.countDown();
                })
                .preconfigureClients(client -> client.config().setAllowSessionExpiry(false))
                .setWaitCondition(tc -> received.get() < packetCount * clientCount);

        try (Infra i = builder.build()) {
            final long millisTaken = i.start().awaitCompletion(1, TimeUnit.MINUTES);
            System.out.println("Receive took " + millisTaken + " ms.");

            assertEquals(packetCount * clientCount, received.get());
        }
    }

    @Test
    public void testBroadcastToEachClient() throws Throwable {
        // TODO implement test
    }

    @Test
    public void testClientsReactToServerShutdown() throws Throwable {
        // TODO implement test
    }

}
