package io.lyracommunity.bolt.performance;

import io.lyracommunity.bolt.helper.TestClient;
import io.lyracommunity.bolt.helper.TestData;
import io.lyracommunity.bolt.helper.TestServer;
import org.junit.Test;

import java.security.MessageDigest;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestBoltLargeData {

    private final NumberFormat format = NumberFormat.getNumberInstance();

    // How many to send
    private final int numPackets = 50;

    // How large (in MB) is a single packet
    private final int size = 20 * 1024 * 1024;

    private volatile long        totalBytesReceived = 0;
    private final Set<Throwable> errors             = new HashSet<>();

    @Test
    public void test1() throws Exception {
        try {
            doTest(0);
        }
        catch (TimeoutException te) {
            te.printStackTrace();
            fail();
        }
    }

    private void doTest(final float packetLossPercentage) throws Exception {
        final MessageDigest serverMD5 = MessageDigest.getInstance("MD5");

        format.setMaximumFractionDigits(2);
        long start = System.currentTimeMillis();

        final long N = numPackets * size;

        final byte[] data = TestData.getRandomData(12345, size);

        final TestServer srv = TestServer.runServer(byte[].class,
                x -> {
                    serverMD5.update(x, 0, x.length);
                    totalBytesReceived += x.length;
                },
                errors::add);
        srv.server.config().setPacketLoss(packetLossPercentage);

        System.out.println("Sending <" + numPackets + "> packets of <" + format.format(size / 1024.0 / 1024.0) + "> Mbytes each");

        final TestClient cli = TestClient.runClient(srv.server.getPort(),
                c -> {
                    try {
                        for (int i = 0; i < numPackets; i++) {
                            long block = System.currentTimeMillis();
                            c.sendBlocking(data);
                            //                                    clientMD5.update(data);
                            double took = System.currentTimeMillis() - block;
                            double arrival = c.getStatistics().getPacketArrivalRate();
                            double snd = c.getStatistics().getSendPeriod();
                            System.out.println("Sent block <" + i + "> in " + took + " ms, "
                                    + " pktArr: " + arrival
                                    + " snd: " + format.format(snd)
                                    + " rate: " + format.format(size / (1024 * took)) + " MB/sec");
                        }
                        c.flush();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                errors::add);

        final MessageDigest clientMD5 = MessageDigest.getInstance("MD5");
        for (int i = 0; i < numPackets; i++) clientMD5.update(data);

        while (totalBytesReceived < N) {
            if (!errors.isEmpty()) throw new RuntimeException(errors.iterator().next());
            Thread.sleep(10);
        }

        long end = System.currentTimeMillis();
        final String md5Sent = TestData.hexString(clientMD5);
        final String md5Received = TestData.hexString(serverMD5);
        System.out.println("Done. Sending " + N / 1024.0 / 1024.0 + " Mbytes took " + (end - start) + " ms");
        final double mbytes = N / (end - start) / 1024.0;
        System.out.println("Rate: " + format.format(mbytes) + " Mbytes/sec " + format.format(8 * mbytes) + " Mbit/sec");
        System.out.println("Server received: " + totalBytesReceived);

        //assertEquals(N,totalBytesReceived);
        System.out.println("MD5 hash of data sent: " + md5Sent);
        System.out.println("MD5 hash of data received: " + md5Received);
        System.out.println(cli.client.getStatistics());

        assertEquals(md5Sent, md5Received);

        cli.cleanup();
        srv.cleanup();

//        // store stat history to csv file
//        client.getStatistics().writeParameterHistory(File.createTempFile("/boltstats-", ".csv"));
    }

}
