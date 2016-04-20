package io.lyracommunity.bolt;

import io.lyracommunity.bolt.event.ConnectionReady;
import io.lyracommunity.bolt.event.PeerDisconnected;
import io.lyracommunity.bolt.helper.Infra;
import io.lyracommunity.bolt.helper.TestClient;
import io.lyracommunity.bolt.helper.TestObjects;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;

/**
 * Created by keen on 24/03/16.
 */
public class MultiClientIT {

    private int numClients;

    @Before
    public void setUp() throws Exception {
        numClients = 2 + new Random().nextInt(3);
        System.out.println("Total of " + numClients + " clients.");
    }

    @Test
    public void testMultiClientDisconnect() throws Throwable {
        final CountDownLatch awaitingConnectionReady = new CountDownLatch(numClients);

        Infra.Builder builder = Infra.Builder.withServerAndClients(numClients)
                .onEventServer((ts, evt) -> System.out.println(evt))
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
                .setWaitCondition(tc -> tc.server().receivedOf(PeerDisconnected.class) < numClients);

        try (Infra i = builder.build()) {
            i.start().awaitCompletion(1, TimeUnit.MINUTES);

            assertEquals(numClients, i.server().receivedOf(PeerDisconnected.class));
        }
    }

    @Test
    public void testReceiveFromMultipleClients() throws Throwable {
        final TestObjects.ReliableUnordered toSend = TestObjects.reliableUnordered(100);
        final int packetCount = 500;
        final boolean sessionExpirable = false;
        final CountDownLatch clientsComplete = new CountDownLatch(numClients);

        Infra.Builder builder = Infra.Builder.withServerAndClients(numClients)
                .preconfigureServer(s -> s.config().setAllowSessionExpiry(sessionExpirable))
                .onReadyClient((tc, rdy) -> {
                    for (int i = 0; i < packetCount; i++) tc.client.send(toSend);
                    tc.client.flush();
                    clientsComplete.countDown();
                })
                .preconfigureClients(client -> client.config().setAllowSessionExpiry(false))
                .setWaitCondition(tc -> tc.server().receivedOf(toSend.getClass()) < packetCount * numClients);

        try (Infra i = builder.build()) {
            i.start().awaitCompletion(1, TimeUnit.MINUTES);

            assertEquals(packetCount * numClients, i.server().receivedOf(toSend.getClass()));
        }
    }

    @Test
    public void testBroadcastToEachClient() throws Throwable {
        final Object toSend = TestObjects.reliableOrdered(100);
        final AtomicInteger awaitingConnection = new AtomicInteger(numClients);
        final Predicate<TestClient> clientPredicate = tc -> (tc.receivedOf(toSend.getClass()) < 1);

        Infra.Builder builder = Infra.Builder.withServerAndClients(numClients)
                .onReadyServer((ts, evt) -> {
                    System.out.println(evt);
                    if (awaitingConnection.decrementAndGet() == 0) {
                        try {
                            ts.server.sendToAll(toSend);
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .preconfigureClients(client -> client.config().setAllowSessionExpiry(false))
                .setWaitCondition(inf -> inf.clients().stream().anyMatch(clientPredicate));

        try (Infra i = builder.build()) {
            i.start().awaitCompletion(1, TimeUnit.MINUTES);

            final long receiveEvents = i.clients().stream().filter(clientPredicate.negate()).count();
            assertEquals(numClients, receiveEvents);
        }
    }

    @Test
    public void testClientsReactToServerShutdown() throws Throwable {
        final AtomicInteger awaitingConnection = new AtomicInteger(numClients);
        final Predicate<TestClient> clientPredicate = tc -> tc.receivedOf(PeerDisconnected.class) < 1;

        Infra.Builder builder = Infra.Builder.withServerAndClients(numClients)
                .onEventServer((ts, evt) -> {
                    System.out.println(evt);
                    if (ConnectionReady.class.equals(evt.getClass())) {
                        if (awaitingConnection.decrementAndGet() == 0) {
                            ts.close();
                        }
                    }
                })
                .preconfigureClients(client -> client.config().setAllowSessionExpiry(false))
                .setWaitCondition(inf -> inf.clients().stream().anyMatch(clientPredicate));

        try (Infra i = builder.build()) {
            i.start().awaitCompletion(1, TimeUnit.MINUTES);

            assertEquals(numClients, i.clients().stream().filter(clientPredicate.negate()).count());
        }
    }

    @Test
    public void testSendOnlyToLastConnectedClient() throws Throwable {
        final Object toSend = TestObjects.reliableOrdered(100);
        final AtomicInteger awaitingConnection = new AtomicInteger(numClients);
        final Predicate<TestClient> clientPredicate = tc -> (tc.receivedOf(toSend.getClass()) < 1);

        Infra.Builder builder = Infra.Builder.withServerAndClients(numClients)
                .onReadyServer((ts, evt) -> {
                    System.out.println(evt);
                    if (awaitingConnection.decrementAndGet() == 0) {
                        try {
                            ts.server.send(toSend, evt.getSession().getSessionID());
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .preconfigureClients(client -> client.config().setAllowSessionExpiry(false))
                .setWaitCondition(inf -> inf.clients().stream().allMatch(clientPredicate));

        try (Infra i = builder.build()) {
            i.start().awaitCompletion(1, TimeUnit.MINUTES);

            final long receiveEvents = i.clients().stream().filter(clientPredicate.negate()).count();
            assertEquals(1, receiveEvents);
        }
    }


}
