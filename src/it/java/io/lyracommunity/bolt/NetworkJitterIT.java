package io.lyracommunity.bolt;

import io.lyracommunity.bolt.helper.TestClient;
import io.lyracommunity.bolt.helper.TestObjects;
import io.lyracommunity.bolt.helper.TestServer;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by omahoc9 on 3/24/16.
 */
public class NetworkJitterIT
{

    @Test
    public void testOrderingWithHighRandomJitter() throws Throwable {
        doTest(400, 100, TestObjects.reliableOrdered(100));
    }

    @Test
    public void testUnorderedPacketsWithJitter() throws Throwable {
        doTest(50, 100, TestObjects.reliableUnordered(100));
    }

    protected void doTest(final int jitterInMillis, final int numPackets, final Object toSend) throws Throwable {
        final AtomicLong startTime = new AtomicLong();

        final TestServer server = TestServer.runObjectServer(toSend.getClass(), null, null);
        server.server.config().setSimulatedMaxJitter(jitterInMillis);

        final TestClient client = TestClient.runClient(server.server.getPort(),
                c -> {
                    System.out.println("Connected, begin send.");
                    startTime.set(System.currentTimeMillis());
                    for (int i = 0; i < numPackets; i++) c.send(toSend);
                }, null);

        while (server.getTotalReceived(TestObjects.ReliableOrdered.class) < numPackets) {
            if (!server.getErrors().isEmpty()) throw server.getErrors().get(0);
            if (!client.getErrors().isEmpty()) throw client.getErrors().get(0);
            Thread.sleep(10);
        }

        final long millisTaken = System.currentTimeMillis() - startTime.get();
        System.out.println("Receive took " + millisTaken + " ms.");

        assertEquals(numPackets, server.getTotalReceived(TestObjects.ReliableOrdered.class));
        assertTrue(jitterInMillis <= millisTaken);

        for (AutoCloseable c : Arrays.asList(server, client)) c.close();
    }

}
