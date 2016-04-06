package io.lyracommunity.bolt;

import io.lyracommunity.bolt.event.PeerDisconnected;
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

/**
 * Created by keen on 24/03/16.
 */
public class MultiClientIT {

    private final Set<Throwable> errors = new HashSet<>();
    private final AtomicInteger received = new AtomicInteger(0);

    @Test
    public void testMultiClientDisconnect() throws Throwable {
        final int numClients = 2 + new Random().nextInt(3);
        final AtomicInteger serverDisconnectedEvents = new AtomicInteger(0);
        final TestServer srv = createServer(evt -> {
            System.out.println(evt);
            if (evt instanceof PeerDisconnected) {
                serverDisconnectedEvents.incrementAndGet();
            }
        });
        final LinkedList<TestClient> clients = new LinkedList<>();
        final CountDownLatch awaitingConnectionReady = new CountDownLatch(numClients);

        clients.addAll(TestClient.runClients(numClients, srv.server.getPort(),
                c -> awaitingConnectionReady.countDown(),
                errors::add,
                null));

        if (!awaitingConnectionReady.await(5, TimeUnit.SECONDS)) throw new RuntimeException("Timed out");

        for (TestClient c : clients) c.close();

        while (errors.isEmpty() && serverDisconnectedEvents.get() < numClients) Thread.sleep(10);

        srv.close();

        assertEquals(numClients, serverDisconnectedEvents.get());
    }

    @Test
    public void testReceiveFromMultipleClients() throws Throwable {
        final TestObjects.ReliableUnordered toSend = TestObjects.reliableUnordered(100);
        final int packetCount = 500;
        final int clientCount = 4;
//        final int clientCount = 2 + new Random().nextInt(4);
        System.out.println("Total of " + clientCount + " clients.");
        final CountDownLatch clientsComplete = new CountDownLatch(clientCount);

        final TestServer srv = createObjectServer(false, toSend);

        final List<TestClient> clients = TestClient.runClients(clientCount, srv.server.getPort(),
                client -> {
                    for (int i = 0; i < packetCount; i++) client.send(toSend);
                    client.flush();
                    clientsComplete.countDown();
                },
                errors::add,
                client -> client.config().setAllowSessionExpiry(false));

        final Supplier<Boolean> done = () -> received.get() < packetCount * clientCount;
        while (done.get() && errors.isEmpty()) Thread.sleep(10);
        if (!errors.isEmpty()) throw new RuntimeException(errors.iterator().next());

        for (AutoCloseable c : clients) c.close();
        srv.close();

        assertEquals(packetCount * clientCount, received.get());
    }

    @Test
    public void testBroadcastToEachClient() throws Throwable {
        // TODO implement test
    }

    @Test
    public void testClientsReactToServerShutdown() throws Throwable {
        // TODO implement test
    }

    private <T> TestServer createObjectServer(final boolean sessionExpirable, final T toSend) throws Exception {
        return TestServer.runObjectServer(toSend.getClass(),
                x -> {
                    if (received.incrementAndGet() % 50 == 0) {
                        System.out.println(MessageFormat.format("Received from [{0}], total {1}.", x.getSessionID(), received.get()));
                    }
                },
                errors::add, server -> server.config().setAllowSessionExpiry(sessionExpirable));
    }

    private TestServer createServer(final Action1<? super Object> onNext) throws Exception {
        return TestServer.runCustomServer(onNext, errors::add, null);
    }

}
