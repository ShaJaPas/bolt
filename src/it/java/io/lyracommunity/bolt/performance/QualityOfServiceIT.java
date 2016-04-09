package io.lyracommunity.bolt.performance;

import io.lyracommunity.bolt.helper.TestClient;
import io.lyracommunity.bolt.helper.TestObjects;
import io.lyracommunity.bolt.helper.TestServer;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by keen on 08/04/16.
 */
public class QualityOfServiceIT {


    /**
     * Set an artificial latency and packet loss and ensure all sent packets take longer
     * than the expected minimum time to deliver.
     */
    @Test
    public void test_HighLatencyAndHighPacketLoss_TimeTakeToDeliveryReliable() throws Throwable {
        doTest(500, 0.2f, 20, TestObjects.reliableUnordered(100), 1500);
    }

    /**
     * Set an artificial latency and packet loss and ensure all sent packets take longer
     * than the expected minimum time to deliver.
     */
    @Test
    public void test_VeryHighLatencyAndHighPacketLoss_TimeTakeToDeliveryReliable() throws Throwable {
        doTest(1000, 0.2f, 5, TestObjects.reliableUnordered(100), 2000);
    }

    /**
     * Set an artificial latency and packet loss and ensure all sent packets take longer
     * than the expected minimum time to deliver.
     */
    @Test
    public void test_LowLatencyAndExtremePacketLoss_TimeTakeToDeliveryReliable() throws Throwable {
        doTest(50, 0.5f, 10, TestObjects.reliableUnordered(100), 100);
    }

    /**
     * Set an artificial latency and packet loss and ensure all sent packets take longer
     * than the expected minimum time to deliver.
     */
    @Test
    public void test_LANLatencyAndMediumPacketLoss_TimeTakeToDeliveryReliable() throws Throwable {
        doTest(5, 0.1f, 10, TestObjects.reliableUnordered(100), 10);
    }


    private void doTest(final int latencyInMillis, final float packetLoss, final int numPackets, final Object toSend,
                        final int minimumExpectedTotalTime) throws Throwable {
        final AtomicLong startTime = new AtomicLong();

        final TestServer server = TestServer.runObjectServer(toSend.getClass(), null, null);
        server.server.config().setSimulatedLatency(latencyInMillis);
        server.server.config().setPacketLoss(packetLoss);

        final TestClient client = TestClient.runClient(server.server.getPort(),
                c -> {
                    System.out.println("Connected, begin send.");
                    startTime.set(System.currentTimeMillis());
                    for (int i = 0; i < numPackets; i++) c.send(toSend);
                }, null);

        while (server.getTotalReceived(toSend.getClass()) < numPackets) {
            if (!server.getErrors().isEmpty()) throw server.getErrors().get(0);
            if (!client.getErrors().isEmpty()) throw client.getErrors().get(0);
            Thread.sleep(2);
        }
        final long millisTaken = System.currentTimeMillis() - startTime.get();
        for (AutoCloseable c : Arrays.asList(server, client)) c.close();

        System.out.println("Receive took " + millisTaken + " ms.");

        assertEquals(numPackets, server.getTotalReceived(toSend.getClass()));

        assertTrue(minimumExpectedTotalTime <= millisTaken);
    }

}
