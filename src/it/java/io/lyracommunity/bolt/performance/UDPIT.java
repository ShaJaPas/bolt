package io.lyracommunity.bolt.performance;

import io.lyracommunity.bolt.Config;
import io.lyracommunity.bolt.helper.PortUtil;
import io.lyracommunity.bolt.packet.DataPacket;
import io.lyracommunity.bolt.packet.DeliveryType;
import io.lyracommunity.bolt.statistic.MeanValue;
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
public class UDPIT
{

    public static final int SERVER_PORT = PortUtil.nextServerPort();
    final int num_packets = 1_000;
    final int packetSize = Config.DEFAULT_DATAGRAM_SIZE;
    private final Queue<DatagramPacket> handoff = new ConcurrentLinkedQueue<>();
    int N = 0;
    long total = 0;
    volatile boolean serverRunning = true;

    @Test
    public void test1() throws Exception {
        // TODO clean up server once complete.
        runServer();
        runThirdThread();

        // Client socket.
        DatagramSocket s = new DatagramSocket(PortUtil.nextClientPort());

        // Generate a test array with random content.
        N = num_packets * packetSize;
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
            Thread.sleep(0, 100);
            dgSendTime.end();
            dgSendInterval.begin();
        }
        System.out.println("Finished sending.");
        while (serverRunning) Thread.sleep(10);
        System.out.println("Server stopped.");
        long end = System.currentTimeMillis();
        System.out.println("Done. Sending " + N / 1024 / 1024 + " Mbytes took " + (end - start) + " ms");
        float rate = N / 1000 / (end - start);
        System.out.println("Rate " + rate + " Mbytes/sec " + (rate * 8) + " Mbit/sec");
        System.out.println("Rate " + num_packets + " packets/sec");
        System.out.println("Mean send time " + dgSendTime.get());
        System.out.println("Mean send interval " + dgSendInterval.get());
        System.out.println("Server received: " + total);
    }

    private void runServer() throws Exception {
        final DatagramSocket serverSocket = new DatagramSocket(SERVER_PORT);

        CompletableFuture.runAsync(() -> {
            try {
                byte[] buf = new byte[packetSize];
                while (true) {
                    DatagramPacket dp = new DatagramPacket(buf, buf.length);
                    serverSocket.receive(dp);
                    handoff.offer(dp);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            serverRunning = false;
        });
        System.out.println("Server started.");
    }

    private void runThirdThread() throws Exception {
        CompletableFuture.runAsync(() -> {
            try {
                int counter = 0;
                long start = System.currentTimeMillis();
                while (counter < num_packets) {
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
