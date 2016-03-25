package bolt;

import bolt.util.ClientUtil;
import bolt.util.ServerUtil;
import bolt.util.TestData;
import bolt.util.TestUtil;
import org.junit.Test;

import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class BoltServerTest {

    int num_packets = 32;
    long total = 0;
    private volatile boolean serverRunning = true;
    private volatile String md5_received = null;
    protected MessageDigest serverMd5;

    @Test(expected = Exception.class)
    public void testErrorTooManyChunks() throws Throwable {
        num_packets = 10_000;
        doTest(0);
    }

    @Test
    public void testPortAlreadyBoundTo() throws Throwable {
        // TODO write test
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
        final BoltServer server = runServer();
        server.config().setPacketLoss(packetLossPercentage);

        final int N = num_packets * 32768;
        final byte[] data = TestData.getRandomData(N);
        final String md5_sent = TestData.computeMD5(data);
        final long start = System.currentTimeMillis();
        final Set<Throwable> errors = new HashSet<>();

        final BoltClient client = ClientUtil.runClient(server.getPort(),
                c -> {
                    System.out.println("Sending data block of <" + N / 1024 + "> Kbytes.");
                    c.sendBlocking(data);
                }, errors::add);

        while (total < N && errors.isEmpty()) Thread.sleep(10);
        if (!errors.isEmpty()) throw errors.iterator().next();

        md5_received = TestUtil.hexString(serverMd5);
        long end = System.currentTimeMillis();
        System.out.println("Shutdown client.");
        System.out.println("Done. Sending " + N / 1024 + " Kbytes took " + (end - start) + " ms");
        System.out.println("Rate " + N / (end - start) + " Kbytes/sec");
        System.out.println("Server received: " + total);
        System.out.println("MD5 hash of data sent: " + md5_sent);
        System.out.println("MD5 hash of data received: " + md5_received);
        System.out.println(client.getStatistics());

        assertEquals(N, total);
        assertEquals(md5_sent, md5_received);
    }

    private BoltServer runServer() throws Exception {
        serverMd5 = MessageDigest.getInstance("MD5");
        return ServerUtil.runServer(byte[].class, x -> {
            serverMd5.update(x, 0, x.length);
            total += x.length;
        }, ex -> {
            System.out.println(ex.toString());
            serverRunning = false;
        });
    }


}
