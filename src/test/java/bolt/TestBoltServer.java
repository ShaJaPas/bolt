package bolt;

import bolt.util.TestUtil;
import bolt.xcoder.MessageAssembleBuffer;
import bolt.xcoder.XCoderRepository;
import org.junit.Test;
import rx.schedulers.Schedulers;

import java.net.InetAddress;
import java.security.MessageDigest;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

public class TestBoltServer extends BoltTestBase {

    public int BUFSIZE = 1024;
    boolean running = false;
    int num_packets = 32;

    int TIMEOUT = 20000;
    long total = 0;
    volatile boolean serverRunning = true;
    volatile String md5_received = null;

    private Config createPacketLossConfig(float packetLossPercentage) {
        final Config config = new Config();
        config.setPacketLoss(0);
        return config;
    }

    @Test
    public void testWithoutLoss() throws Exception {
        Logger.getLogger("bolt").setLevel(Level.INFO);
        num_packets = 1000;
        TIMEOUT = Integer.MAX_VALUE;
        doTest(createPacketLossConfig(0));
    }

    // Set an artificial loss rate.
    @Test
    public void testWithLoss() throws Exception {
        TIMEOUT = Integer.MAX_VALUE;
        num_packets = 100;
        //set log level
        Logger.getLogger("bolt").setLevel(Level.INFO);
        doTest(createPacketLossConfig(0.33334f));
    }

    // Send even more data.
    @Test
    public void testLargeDataSet() throws Exception {
        TIMEOUT = Integer.MAX_VALUE;
        num_packets = 100;
        //set log level
        Logger.getLogger("bolt").setLevel(Level.INFO);
        doTest(createPacketLossConfig(0));

    }

    protected void doTest(final Config config) throws Exception {
        if (!running) runServer();
        BoltClient client = new BoltClient(InetAddress.getByName("localhost"), 12345, config);
        client.connect(InetAddress.getByName("localhost"), 65321);
        int N = num_packets * 32768;
        byte[] data = new byte[N];
        new Random().nextBytes(data);

        while (!serverRunning) Thread.sleep(100);

        String md5_sent = computeMD5(data);
        long start = System.currentTimeMillis();
        System.out.println("Sending data block of <" + N / 1024 + "> Kbytes.");

        if (serverRunning) {
            client.sendBlocking(data);
        }
        else throw new IllegalStateException();

        long end = System.currentTimeMillis();
        System.out.println("Shutdown client.");
        client.shutdown();

        while (serverRunning) Thread.sleep(100);

        System.out.println("Done. Sending " + N / 1024 + " Kbytes took " + (end - start) + " ms");
        System.out.println("Rate " + N / (end - start) + " Kbytes/sec");
        System.out.println("Server received: " + total);

        assertEquals(N, total);
        System.out.println("MD5 hash of data sent: " + md5_sent);
        System.out.println("MD5 hash of data received: " + md5_received);
        System.out.println(client.getStatistics());

        assertEquals(md5_sent, md5_received);
    }

    private void runServer() throws Exception {
        final MessageDigest md5 = MessageDigest.getInstance("MD5");
        final BoltServer server = new BoltServer(XCoderRepository.create(new MessageAssembleBuffer()));

        server.bind(InetAddress.getByName("localhost"), 65321)
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .ofType(byte[].class)
                .timeout(1, TimeUnit.SECONDS)
                .subscribe(x -> {
                            md5.update(x, 0, x.length);
                            total += x.length;
                        },
                        ex -> {
                            System.out.println(ex.toString());
                            serverRunning = false;
                        },
                        () -> {
                            serverRunning = false;
                            md5_received = TestUtil.hexString(md5);
                        });
    }

}
