package io.lyracommunity.bolt.performance;

import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.helper.PortUtil;
import io.lyracommunity.bolt.packet.DataPacket;
import io.lyracommunity.bolt.packet.DeliveryType;
import io.lyracommunity.bolt.statistic.MeanValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * send some data over a UDP connection and measure performance
 */
public class UDPIT {

    private static final int                   SERVER_PORT   = PortUtil.nextServerPort();
    private final        int                   packetSize    = Config.DEFAULT_DATAGRAM_SIZE - DataPacket.MAX_HEADER_SIZE;
    private final        int                   num_packets   = 10_000;
    private final        Queue<DatagramPacket> handoff       = new ConcurrentLinkedQueue<>();
    private volatile     boolean               serverRunning = true;
    private              long                  total         = 0;
    private DatagramSocket serverSocket;

    @Before
    public void setUp() throws Exception {
        serverSocket = new DatagramSocket(SERVER_PORT);
    }

    @After
    public void tearDown() throws Exception {
        serverSocket.close();
    }

    @Test
    public void test1() throws Exception {
        runServer();
        runThirdThread();

        // Client socket.
        DatagramSocket s = new DatagramSocket(PortUtil.nextClientPort());

        // Generate a test array with random content.
        int n = num_packets * packetSize;
        byte[] data = new byte[packetSize];
        new Random().nextBytes(data);
        long start = System.currentTimeMillis();
        DatagramPacket dp = new DatagramPacket(new byte[packetSize], packetSize);
        dp.setAddress(InetAddress.getByName("localhost"));
        dp.setPort(SERVER_PORT);
        System.out.println("Sending " + num_packets + " data blocks of <" + packetSize + "> bytes");
        MeanValue dgSendTime = new MeanValue("Datagram send time");
        MeanValue dgSendInterval = new MeanValue("Datagram send interval");

        for (int i = 0; i < num_packets; i++) {
            DataPacket p = new DataPacket();
            p.setDelivery(DeliveryType.UNRELIABLE_UNORDERED);
            p.setData(data);
            dp.setData(p.getEncoded());
            dgSendInterval.end();
            dgSendTime.begin();
            s.send(dp);
//            Thread.sleep(0, 100);
            dgSendTime.end();
            dgSendInterval.begin();
        }
        System.out.println("Finished sending.");
//        while (serverRunning) Thread.sleep(10);
        System.out.println("Server stopped.");
        long end = System.currentTimeMillis();
        System.out.println("Done. Sending " + n / 1024 / 1024 + " Mbytes took " + (end - start) + " ms");
        float rate = n / 1000 / (end - start);
        System.out.println("Rate " + rate + " Mbytes/sec " + (rate * 8) + " Mbit/sec");
        System.out.println("Rate " + num_packets + " packets/sec");
        System.out.println("Mean send time " + dgSendTime.get());
        System.out.println("Mean send interval " + dgSendInterval.get());
        System.out.println("Server received: " + total);

        serverRunning = false;
    }

    private void runServer() throws Exception {

        CompletableFuture.runAsync(() -> {
            try {
                final byte[] buf = new byte[packetSize];
                serverRunning = true;
                while (serverRunning) {
                    DatagramPacket dp = new DatagramPacket(buf, buf.length);
                    serverSocket.receive(dp);
                    handoff.offer(dp);
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            finally {
                serverRunning = false;
            }
        });
        System.out.println("Server started.");
    }

    private void runThirdThread() throws Exception {
        CompletableFuture.runAsync(() -> {
            try {
                int counter = 0;
                long start = System.currentTimeMillis();
                while (counter < num_packets && serverRunning) {
                    DatagramPacket dp = handoff.poll();
                    if (dp != null) {
                        total += dp.getLength();
                        counter++;
                    }
                }
                long end = System.currentTimeMillis();
                System.out.println("Count: " + counter);
                System.out.println("Server time: " + (end - start) + " ms.");

            }
            catch (Exception e) {
                e.printStackTrace();
            }
            serverRunning = false;
        });
        System.out.println("Hand-off thread started.");

    }

}
