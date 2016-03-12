package bolt.performance;

import bolt.BoltClient;
import bolt.BoltServer;
import bolt.BoltTestBase;
import bolt.Config;
import bolt.util.TestUtil;
import bolt.xcoder.MessageAssembleBuffer;
import bolt.xcoder.XCoderRepository;
import org.junit.Test;
import rx.schedulers.Schedulers;

import java.io.File;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.text.NumberFormat;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestBoltLargeData extends BoltTestBase {

    public static final int SERVER_PORT = 65318;
    private final NumberFormat format = NumberFormat.getNumberInstance();
    boolean running = false;
    //how many
    int num_packets = 50;
    //how large is a single packet
    int size = 20 * 1024 * 1024;
    int TIMEOUT = Integer.MAX_VALUE;
    int READ_BUFFERSIZE = 1 * 1024 * 1024;
    long total = 0;
    volatile boolean serverRunning = true;
    volatile boolean serverStarted = false;
    volatile String md5_received = null;

    @Test
    public void test1() throws Exception {
        Logger.getLogger("bolt").setLevel(Level.INFO);
//		System.setProperty("bolt.receiver.storeStatistics","true");
//		System.setProperty("bolt.sender.storeStatistics","true");
//        System.setProperty(BoltSession.CC_CLASS, SimpleTCP.class.getName());
        TIMEOUT = Integer.MAX_VALUE;
        try {
            doTest(0);
        } catch (TimeoutException te) {
            te.printStackTrace();
            fail();
        }
    }

    protected void doTest(final float packetLossPercentage) throws Exception {
        final Config config = new Config(InetAddress.getByName("localhost"), 12345)
                .setPacketLoss(packetLossPercentage);

        format.setMaximumFractionDigits(2);

        if (!running) runServer();
        BoltClient client = new BoltClient(config);
        client.connect(InetAddress.getByName("localhost"), SERVER_PORT);

        long N = num_packets * size;

        byte[] data = new byte[size];
        new Random().nextBytes(data);

        MessageDigest digest = MessageDigest.getInstance("MD5");
        while (!serverStarted) Thread.sleep(100);
        long start = System.currentTimeMillis();
        System.out.println("Sending <" + num_packets + "> packets of <" + format.format(size / 1024.0 / 1024.0) + "> Mbytes each");
        long end;

        if (serverRunning) {
            for (int i = 0; i < num_packets; i++) {
                long block = System.currentTimeMillis();
                client.send(data);
                client.flush();
                digest.update(data);
                double took = System.currentTimeMillis() - block;
                double arrival = client.getStatistics().getPacketArrivalRate();
                double snd = client.getStatistics().getSendPeriod();
                System.out.println("Sent block <" + i + "> in " + took + " ms, "
                        + " pktArr: " + arrival
                        + " snd: " + format.format(snd)
                        + " rate: " + format.format(size / (1024 * took)) + " MB/sec");
            }
            client.flush();
            end = System.currentTimeMillis();
            client.shutdown();
        } else throw new IllegalStateException();
        String md5_sent = hexString(digest);

        while (serverRunning) Thread.sleep(100);

        System.out.println("Done. Sending " + N / 1024 / 1024 + " Mbytes took " + (end - start) + " ms");
        double mbytes = N / (end - start) / 1024.0;
        double mbit = 8 * mbytes;
        System.out.println("Rate: " + format.format(mbytes) + " Mbytes/sec " + format.format(mbit) + " Mbit/sec");
        System.out.println("Server received: " + total);

        //	assertEquals(N,total);
        System.out.println("MD5 hash of data sent: " + md5_sent);
        System.out.println("MD5 hash of data received: " + md5_received);
        System.out.println(client.getStatistics());

        assertEquals(md5_sent, md5_received);

        //store stat history to csv file
        client.getStatistics().writeParameterHistory(File.createTempFile("/boltstats-", ".csv"));
    }

    private void runServer() throws Exception {
        final long start = System.currentTimeMillis();
        final MessageDigest md5 = MessageDigest.getInstance("MD5");
        final BoltServer server = new BoltServer(new Config(InetAddress.getByName("localhost"), SERVER_PORT));
        server.bind()
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .ofType(byte[].class)
                .subscribe(x -> {
                    md5.update(x, 0, x.length);
                    total += x.length;
                });
        serverRunning = false; //TODO rewrite runServer method correctly.
        md5_received = TestUtil.hexString(md5);
//
//        Runnable serverProcess = () -> {
//
//            try {
//                BoltSocket s = serverSocket.accept();
//                serverStarted = true;
//                assertNotNull(s);
//                BoltInputStream is = s.getInputStream();
//                byte[] buf = new byte[READ_BUFFERSIZE];
//                int c = 0;
//                while (true) {
//                    if (checkTimeout(start)) break;
//                    c = is.read(buf);
//                    if (c < 0) break;
//                    else {
//                        md5.update(buf, 0, c);
//                    }
//                }
//                System.out.println("Server thread exiting, last received bytes: " + c);
//                serverSocket.shutDown();
//                System.out.println(s.getSession().getStatistics());
//            } catch (Exception e) {
//                e.printStackTrace();
//                fail();
//                serverRunning = false;
//            }
//        };
//        Thread t = new Thread(serverProcess);
//        t.start();
//    }
//
//
//    private boolean checkTimeout(long start) {
//        boolean to = System.currentTimeMillis() - start > TIMEOUT;
//        if (to) System.out.println("TIMEOUT");
//        return to;
    }
}
