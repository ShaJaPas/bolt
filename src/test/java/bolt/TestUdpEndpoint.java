package bolt;

import bolt.packets.Destination;
import bolt.util.PortUtil;
import org.junit.Test;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestUdpEndpoint extends BoltTestBase {

    public final int SERVER_PORT = PortUtil.nextServerPort();
    public final int CLIENT_PORT = PortUtil.nextClientPort();

    @Test
    public void testClientServerMode() throws Exception {
        final int numPackets = 50 + new Random().nextInt(50);

        final BoltEndPoint server = new BoltEndPoint(InetAddress.getByName("localhost"), SERVER_PORT);
        final Subscription endpointSub = server.start()
                .observeOn(Schedulers.computation())
                .subscribe();

        final BoltClient client = runClient(SERVER_PORT,
                c -> IntStream.range(0, numPackets).forEach(__ -> c.send(getRandomData(1024))),
                ex -> System.out.println(ex));

        // Pause until all are delivered
        boolean done = false;
        while (!done) {
            Thread.sleep(10);
            List<BoltSession> sessions = new ArrayList<>(server.getSessions());
            BoltSocket s = sessions.isEmpty() ? null : sessions.get(0).getSocket();
            done = (s != null && s.getReceiveBuffer().getNumChunks() >= numPackets);
        }

        System.out.println(client.getStatistics());
        System.out.println(server.getSessions().iterator().next().getStatistics());

        int sent = client.getStatistics().getNumberOfSentDataPackets();
        int received = server.getSessions().iterator().next().getStatistics().getNumberOfReceivedDataPackets();
        assertEquals(numPackets, sent);
        assertEquals(numPackets, received);
    }

    /**
     * Just check how fast we can send out UDP packets from the endpoint.
     *
     * @throws Exception
     */
    @Test
    public void testRawSendRate() throws Exception {
        System.out.println("Checking raw UDP send rate...");
        InetAddress localhost = InetAddress.getByName("localhost");
        BoltEndPoint endpoint = new BoltEndPoint(localhost, SERVER_PORT);
        Subscription sub = endpoint.start().subscribe();
        Destination d1 = new Destination(localhost, CLIENT_PORT);
        final int dataSize = BoltEndPoint.DATAGRAM_SIZE;
        final DatagramPacket p = new DatagramPacket(getRandomData(dataSize), dataSize, d1.getAddress(), d1.getPort());
        final int N = 100_000;

        // Send many packets as fast as we can
        long start = System.currentTimeMillis();
        for (int i = 0; i < N; i++) {
            endpoint.sendRaw(p);
        }
        long end = System.currentTimeMillis();

        // Print results
        float rate = 1000 * N / (end - start);
        System.out.println("PacketRate: " + (int) rate + " packets/sec.");
        float dataRate = dataSize * rate / 1024 / 1024;
        System.out.println("Data Rate:  " + (int) dataRate + " MBytes/sec.");

        sub.unsubscribe();
    }

    // @Test()
    public void testRendezvousConnect() throws Exception {

    }

    @Test
    public void testBindToAnyPort() throws Exception {
        BoltEndPoint ep = new BoltEndPoint(InetAddress.getByName("localhost"), 0);
        int port = ep.getLocalPort();
        assertTrue(port > 0);
    }

}
