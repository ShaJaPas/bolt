package io.lyracommunity.bolt;

import io.lyracommunity.bolt.helper.Infra;
import io.lyracommunity.bolt.helper.TestData;
import io.lyracommunity.bolt.packet.DataPacket;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by omahoc9 on 3/24/16.
 */
public class NetworkBandwidthIT
{

    /**
     * Set an artificial bandwidth and ensure all sent packets take longer than minimum to deliver.
     */
    @Test
    public void test_VeryLowBandwidth_SlowYetResponsive() throws Throwable {
        doTest(2, 10);
    }

    /**
     * Set an artificial bandwidth and ensure all sent packets take longer than minimum to deliver.
     */
    @Test
    public void test_MediumBandwidth_SlowYetResponsive() throws Throwable {
        doTest(80, 400);
    }

    /**
     * Set an artificial bandwidth and ensure all sent packets take longer than minimum to deliver.
     */
    @Test
    public void test_HighBandwidthAndHighThroughput_SlowYetResponsive() throws Throwable {
        doTest(1000, 4000);
    }

    private void doTest(final int bandwidthKilobytesPerSec, int numPackets) throws Throwable {
        final Object testData = TestData.getRandomData(1000 - DataPacket.MAX_HEADER_SIZE);
        final long expectedMinimumTime = Math.round(0.95f * 1000f * ((numPackets - bandwidthKilobytesPerSec) / (float) bandwidthKilobytesPerSec));

        Infra.InfraBuilder builder = Infra.InfraBuilder.withServerAndClients(1)
                .preconfigureServer(s -> s.config().setSimulatedBandwidth(bandwidthKilobytesPerSec))
                .onReadyClient((tc, rdy) -> {
                    System.out.println("Connected, begin send.");
                    for (int i = 0; i < numPackets; i++) tc.client.send(testData);
                })
                .setWaitCondition(tc -> tc.getTotalReceived(testData.getClass()) < numPackets);

        try (Infra i = builder.build()) {
            final long millisTaken = i.start().awaitCompletion(expectedMinimumTime * 2, TimeUnit.MILLISECONDS);

            System.out.println("Expected minimum time: " + expectedMinimumTime + " ms.");
            System.out.println("Receive took " + millisTaken + " ms.");

            assertEquals(numPackets, i.getServer().getTotalReceived(testData.getClass()));
            assertTrue(expectedMinimumTime <= millisTaken);
        }
    }

}
