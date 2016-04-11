package io.lyracommunity.bolt;

import io.lyracommunity.bolt.helper.Infra;
import io.lyracommunity.bolt.helper.TestClient;
import io.lyracommunity.bolt.helper.TestData;
import io.lyracommunity.bolt.helper.TestServer;
import org.junit.Test;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BoltServerIT
{

    private int num_packets = 32;
    private long total = 0;
    private volatile long totalReceived = 0;
    private MessageDigest serverMd5;

    @Test(expected = Exception.class)
    public void testErrorTooManyChunks() throws Throwable {
        num_packets = 10_000;
        doTest(0);
    }

    @Test
    public void testWithoutLoss() throws Throwable {
        num_packets = 1000;
        doTest(0);
    }

    // Set an artificial loss rate.
    @Test
    public void testWithHighLoss() throws Throwable {
        num_packets = 100;
        doTest(0.5f);
    }

    // Set an artificial loss rate.
    @Test
    public void testWithLowLoss() throws Throwable {
        num_packets = 100;
        doTest(0.1f);
    }

    // Send even more data.
    @Test
    public void testLargeDataSet() throws Throwable {
        num_packets = 100;
        doTest(0);
    }


    private void doTest(final float packetLossPercentage) throws Throwable {

        final int N = num_packets * 32768;
        final byte[] data = TestData.getRandomData(N);
        final String md5_sent = TestData.computeMD5(data);

        Infra.InfraBuilder builder = Infra.InfraBuilder.withServerAndClients(1)
                .preconfigureServer(s -> s.config().setPacketLoss(packetLossPercentage))
                .onEventServer((ts, evt) -> {
                    if (byte[].class.equals(evt.getClass())) {
                        byte[] x = (byte[]) evt;
                        totalReceived++;
                        if (totalReceived % 10_000 == 0)
                            System.out.println("Received: " + totalReceived);
                        serverMd5.update(x, 0, x.length);
                        total += x.length;
                    }
                })
                .onReadyClient((tc, evt) -> {
                    System.out.println("Sending data block of <" + N / 1024 + "> Kbytes.");
                    tc.client.sendBlocking(data);
                })
                .setWaitCondition(ts -> ts.getTotalReceived(byte[].class) < 1);

        try (Infra i = builder.build()) {
            final long millisTaken = i.start().awaitCompletion();
            System.out.println("Receive took " + millisTaken + " ms.");

            final String md5_received = TestData.hexString(serverMd5);
            System.out.println("Shutdown client.");
            System.out.println("Done. Sending " + N / 1024 + " Kbytes took " + (millisTaken) + " ms");
            System.out.println("Rate " + N / (millisTaken) + " Kbytes/sec");
            System.out.println("Server received: " + total);
            System.out.println("MD5 hash of data sent: " + md5_sent);
            System.out.println("MD5 hash of data received: " + md5_received);

            assertEquals(N, total);
            assertEquals(md5_sent, md5_received);
        }
    }


}
