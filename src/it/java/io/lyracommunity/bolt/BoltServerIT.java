package io.lyracommunity.bolt;

import io.lyracommunity.bolt.helper.TestClient;
import io.lyracommunity.bolt.helper.TestData;
import io.lyracommunity.bolt.helper.TestServer;
import org.junit.Test;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

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

    protected void doTest(final float packetLossPercentage) throws Throwable {
        final TestServer server = runServer();
        server.server.config().setPacketLoss(packetLossPercentage);

        final int N = num_packets * 32768;
        final byte[] data = TestData.getRandomData(N);
        final String md5_sent = TestData.computeMD5(data);
        final long start = System.currentTimeMillis();
        final Set<Throwable> errors = new HashSet<>();

        final TestClient client = TestClient.runClient(server.server.getPort(),
                c -> {
                    System.out.println("Sending data block of <" + N / 1024 + "> Kbytes.");
                    c.sendBlocking(data);
                }, errors::add);

        while (total < N && errors.isEmpty()) Thread.sleep(10);
        if (!errors.isEmpty()) throw errors.iterator().next();

        final String md5_received = TestData.hexString(serverMd5);
        long end = System.currentTimeMillis();
        System.out.println("Shutdown client.");
        System.out.println("Done. Sending " + N / 1024 + " Kbytes took " + (end - start) + " ms");
        System.out.println("Rate " + N / (end - start) + " Kbytes/sec");
        System.out.println("Server received: " + total);
        System.out.println("MD5 hash of data sent: " + md5_sent);
        System.out.println("MD5 hash of data received: " + md5_received);
        System.out.println(client.client.getStatistics());

        assertEquals(N, total);
        assertEquals(md5_sent, md5_received);

        for (AutoCloseable c : Arrays.asList(server, client)) c.close();
    }

    private TestServer runServer() throws Exception {
        serverMd5 = MessageDigest.getInstance("MD5");
        return TestServer.runObjectServer(byte[].class, x -> {
            totalReceived++;
            if (totalReceived % 10_000 == 0) System.out.println("Received: " + totalReceived);
            serverMd5.update(x.getPayload(), 0, x.getPayload().length);
            total += x.getPayload().length;
        }, ex -> {
            System.out.println(ex.toString());
        });
    }


}
