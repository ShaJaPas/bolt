package io.lyracommunity.bolt;

import io.lyracommunity.bolt.helper.Infra;
import io.lyracommunity.bolt.helper.TestObjects;
import io.lyracommunity.bolt.helper.TestObjects.ReliableOrdered;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by omahoc9 on 3/24/16.
 */
public class NetworkLatencyIT
{

    private static final int NUM_PACKETS = 10;

    /**
     * Set an artificial latency and ensure all sent packets take longer than the latency to deliver.
     */
    @Test
    public void test_LowLatency_ResponsiveAfterDelay() throws Throwable {
        doTest(50);
    }

    /**
     * Set an artificial latency and ensure all sent packets take longer than the latency to deliver.
     */
    @Test
    public void test_MediumLatency_ResponsiveAfterDelay() throws Throwable {
        doTest(200);
    }

    /**
     * Set an artificial latency and ensure all sent packets take longer than the latency to deliver.
     */
    @Test
    public void test_HighLatency_ResponsiveAfterDelay() throws Throwable {
        doTest(500);
    }

    /**
     * Set an artificial latency and ensure all sent packets take longer than the latency to deliver.
     */
    @Test
    public void test_VeryHighLatency_ResponsiveAfterDelay() throws Throwable {
        doTest(1000);
    }

    /**
     * Set an artificial latency and ensure all sent packets take longer than the latency to deliver.
     */
    @Test
    public void test_ExtremeLatency_ResponsiveAfterDelay() throws Throwable {
        doTest(2000);
    }
//
//    private void doTest(final int latencyInMillis) throws Throwable {
//        final ReliableOrdered testData = TestObjects.reliableOrdered(100);
//        final AtomicLong startTime = new AtomicLong();
//
//        final TestServer server = TestServer.runObjectServer(testData.getClass(), null, null);
//        server.server.config().setSimulatedLatency(latencyInMillis);
//
//        final TestClient client = TestClient.runClient(server.server.getPort(),
//                c -> {
//                    System.out.println("Connected, begin send.");
//                    startTime.set(System.currentTimeMillis());
//                    for (int i = 0; i < NUM_PACKETS; i++) c.send(testData);
//                });
//
//        while (server.getTotalReceived(ReliableOrdered.class) < NUM_PACKETS) {
//            if (!server.getErrors().isEmpty()) throw server.getErrors().get(0);
//            if (!client.getErrors().isEmpty()) throw client.getErrors().get(0);
//            Thread.sleep(10);
//        }
//
//        final long millisTaken = System.currentTimeMillis() - startTime.get();
//        System.out.println("Receive took " + millisTaken + " ms.");
//
//        assertEquals(NUM_PACKETS, server.getTotalReceived(ReliableOrdered.class));
//        assertTrue(latencyInMillis <= millisTaken);
//
//        for (AutoCloseable c : Arrays.asList(server, client)) c.close();
//    }


    private void doTest(final int latencyInMillis) throws Throwable {
        final ReliableOrdered testData = TestObjects.reliableOrdered(100);

        Infra.InfraBuilder builder = Infra.InfraBuilder.withServerAndClients(1)
                .preconfigureServer(s -> s.config().setSimulatedLatency(latencyInMillis))
                .onReadyClient((tc, rdy) -> {
                    System.out.println("Connected, begin send.");
                    for (int i = 0; i < NUM_PACKETS; i++) tc.client.send(testData);
                })
                .setWaitCondition(tc -> tc.getTotalReceived(testData.getClass()) < NUM_PACKETS);

        try (Infra i = builder.build()) {
            i.start();
            final long millisTaken = i.awaitCompletion();
            System.out.println("Receive took " + millisTaken + " ms.");

            assertEquals(NUM_PACKETS, i.getServer().getTotalReceived(ReliableOrdered.class));
            assertTrue(latencyInMillis <= millisTaken);
        }
    }



}
