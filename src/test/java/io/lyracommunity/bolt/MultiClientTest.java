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
public class MultiClientTest {

    private final Set<Throwable> errors = new HashSet<>();
    private final AtomicInteger received = new AtomicInteger(0);

    @Test
    public void testSingleClientDisconnect() throws Throwable {

    }

    @Test
    public void testReceiveFromMultipleClients() throws Throwable {
        final TestPackets.ReliableUnordered toSend = TestPackets.reliableUnordered(100);
        final int PACKET_COUNT = 500;
        final int clientCount = 2 + new Random().nextInt(4);
        System.out.println("Total of " + clientCount + " clients.");
        final CountDownLatch clientsComplete = new CountDownLatch(clientCount);

        final TestServer srv = TestServer.runServer(toSend.getClass(),
                x -> {
                    if (received.incrementAndGet() % 50 == 0) {
                        System.out.println(MessageFormat.format("Received from [{0}], total {1}.", x.getSessionID(), received.get()));
                    }
                },
                errors::add);
        srv.server.config().setSessionsExpirable(false);

        System.out.println("Connect to server port " + srv.server.getPort());
        final List<TestClient> clients = new ArrayList<>();
        for (int j = 0; j < clientCount; j++) {
            final TestClient cli1 = TestClient.runClient(srv.server.getPort(),
                    c -> {
                        for (int i = 0; i < PACKET_COUNT; i++) {
                            c.send(toSend);
                        }
                        c.flush();
                        clientsComplete.countDown();
                    },
                    errors::add);
            cli1.client.config().setSessionsExpirable(false);
            clients.add(cli1);
        }

        final Supplier<Boolean> done = () -> received.get() < PACKET_COUNT * clientCount;
        while (done.get() && errors.isEmpty()) Thread.sleep(10);
        if (!errors.isEmpty()) throw new RuntimeException(errors.iterator().next());

        clients.forEach(c -> c.printStatistics().cleanup());
        srv.printStatistics().cleanup();

        assertEquals(PACKET_COUNT * clientCount, received.get());
    }

    @Test
    public void testBroadcastToEachClient() throws Throwable {

    }

    @Test
    public void testClientsReactToServerShutdown() throws Throwable {

    }


}
