package io.lyracommunity.bolt;

import io.lyracommunity.bolt.helper.TestClient;
import io.lyracommunity.bolt.helper.TestPackets;
import io.lyracommunity.bolt.helper.TestServer;
import org.junit.Test;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

/**
 * Created by keen on 24/03/16.
 */
public class MultiClientIT
{

    private final Set<Throwable> errors = new HashSet<>();
    private final AtomicInteger received = new AtomicInteger(0);

    @Test
    public void testSingleClientDisconnect() throws Throwable {

    }

    @Test
    public void testReceiveFromMultipleClients() throws Throwable {
        final TestPackets.ReliableUnordered toSend = TestPackets.reliableUnordered(100);
        final int packetCount = 500;
        final int clientCount = 2 + new Random().nextInt(4);
        System.out.println("Total of " + clientCount + " clients.");
        final CountDownLatch clientsComplete = new CountDownLatch(clientCount);

        final TestServer srv = createServer(false, toSend);

        final List<TestClient> clients = TestClient.runClients(clientCount, srv.server.getPort(),
                client -> {
                    for (int i = 0; i < packetCount; i++) client.send(toSend);
                    client.flush();
                    clientsComplete.countDown();
                },
                errors::add,
                client -> client.config().setSessionsExpirable(false));

        final Supplier<Boolean> done = () -> received.get() < packetCount * clientCount;
        while (done.get() && errors.isEmpty()) Thread.sleep(10);
        if (!errors.isEmpty()) throw new RuntimeException(errors.iterator().next());

        clients.forEach(c -> c.printStatistics().cleanup());
        srv.printStatistics().cleanup();

        assertEquals(packetCount * clientCount, received.get());
    }

    @Test
    public void testBroadcastToEachClient() throws Throwable {

    }

    @Test
    public void testClientsReactToServerShutdown() throws Throwable {

    }

    private <T> TestServer createServer(final boolean sessionExpirable, final T toSend) throws Exception {
        return TestServer.runServer(toSend.getClass(),
                x -> {
                    if (received.incrementAndGet() % 50 == 0) {
                        System.out.println(MessageFormat.format("Received from [{0}], total {1}.", x.getSessionID(), received.get()));
                    }
                },
                errors::add, server -> server.config().setSessionsExpirable(sessionExpirable));
    }

}
