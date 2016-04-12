package io.lyracommunity.bolt;

import io.lyracommunity.bolt.helper.Infra;
import io.lyracommunity.bolt.helper.TestData;
import io.lyracommunity.bolt.packet.DataPacket;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by omahoc9 on 3/24/16.
 */
public class NetworkBandwidthIT
{

    private static final int NUM_PACKETS = 10;

    /**
     * Set an artificial latency and ensure all sent packets take longer than the latency to deliver.
     */
    @Test
    public void test_LowLatency_ResponsiveAfterDelay() throws Throwable {
        doTest(1000);
    }

    private void doTest(final int bandwidthKilobytesPerSec) throws Throwable {
        final Object testData = TestData.getRandomData(1024 - DataPacket.MAX_HEADER_SIZE);

        Infra.InfraBuilder builder = Infra.InfraBuilder.withServerAndClients(1)
                .preconfigureServer(s -> s.config().setSimulatedBandwidth(bandwidthKilobytesPerSec))
                .onReadyClient((tc, rdy) -> {
                    System.out.println("Connected, begin send.");
                    for (int i = 0; i < NUM_PACKETS; i++) tc.client.send(testData);
                })
                .setWaitCondition(tc -> tc.getTotalReceived(testData.getClass()) < NUM_PACKETS);

        try (Infra i = builder.build()) {
            final long millisTaken = i.start().awaitCompletion();
            System.out.println("Receive took " + millisTaken + " ms.");

            assertEquals(NUM_PACKETS, i.getServer().getTotalReceived(testData.getClass()));
            assertTrue(bandwidthKilobytesPerSec <= millisTaken);
        }
    }


}
