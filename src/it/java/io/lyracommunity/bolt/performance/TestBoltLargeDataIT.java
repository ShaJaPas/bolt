package io.lyracommunity.bolt.performance;

import io.lyracommunity.bolt.helper.Infra;
import io.lyracommunity.bolt.helper.TestData;
import io.lyracommunity.bolt.helper.TestObjects;
import org.junit.Test;

import java.security.MessageDigest;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestBoltLargeDataIT
{

    private final NumberFormat format = NumberFormat.getNumberInstance();

    // How many to send
    private final int numPackets = 50;

    // How large (in MB) is a single packet
    private final int size = 20 * 1024 * 1024;

    private volatile long        totalBytesReceived = 0;

    @Test
    public void test1() throws Throwable {
        try {
            doTest(0);
        }
        catch (TimeoutException te) {
            te.printStackTrace();
            fail();
        }
    }

    private void doTest(final int packetLossPercentage) throws Throwable {
        final MessageDigest serverMD5 = MessageDigest.getInstance("MD5");

        format.setMaximumFractionDigits(2);

        final long N = numPackets * size;

        final byte[] data = TestData.getRandomData(12345, size);

        Infra.InfraBuilder builder = Infra.InfraBuilder.withServerAndClients(1)
                .preconfigureServer(s -> s.config().setPacketLoss(packetLossPercentage))
                .onEventServer((ts, evt) -> {
                    if (byte[].class.equals(evt.getClass())) {
                        byte[] x = (byte[]) evt;
                        serverMD5.update(x, 0, x.length);
                        totalBytesReceived += x.length;
                    }
                })
                .onReadyClient((tc, rdy) -> {
                    try {
                        for (int i = 0; i < numPackets; i++) {
                            long block = System.currentTimeMillis();
                            tc.client.sendBlocking(data);
                            //                                    clientMD5.update(data);
                            double took = System.currentTimeMillis() - block;
                            double arrival = tc.client.getStatistics().getPacketArrivalRate();
                            double snd = tc.client.getStatistics().getSendPeriod();
                            System.out.println("Sent block <" + i + "> in " + took + " ms, "
                                    + " pktArr: " + arrival
                                    + " snd: " + format.format(snd)
                                    + " rate: " + format.format(size / (1024 * took)) + " MB/sec");
                        }
                        tc.client.flush();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .setWaitCondition(tc -> totalBytesReceived < N);


        final MessageDigest clientMD5 = MessageDigest.getInstance("MD5");
        for (int i = 0; i < numPackets; i++) clientMD5.update(data);

        System.out.println("Sending <" + numPackets + "> packets of <" + format.format(size / 1024.0 / 1024.0) + "> Mbytes each");

        try (Infra i = builder.build()) {
            final long millisTaken = i.start().awaitCompletion(4, TimeUnit.MINUTES);

            final String md5Sent = TestData.hexString(clientMD5);
            final String md5Received = TestData.hexString(serverMD5);
            System.out.println("Done. Sending " + N / 1024.0 / 1024.0 + " Mbytes took " + (millisTaken) + " ms");
            final double mbytes = N / (millisTaken) / 1024.0;
            System.out.println("Rate: " + format.format(mbytes) + " Mbytes/sec " + format.format(8 * mbytes) + " Mbit/sec");
            System.out.println("Server received: " + totalBytesReceived);

            //assertEquals(N,totalBytesReceived);
            System.out.println("MD5 hash of data sent: " + md5Sent);
            System.out.println("MD5 hash of data received: " + md5Received);

            assertEquals(md5Sent, md5Received);
        }
    }

}
