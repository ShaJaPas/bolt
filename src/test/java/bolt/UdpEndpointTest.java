package bolt;

import bolt.packet.Destination;
import bolt.helper.ClientUtil;
import bolt.helper.PortUtil;
import bolt.helper.TestData;
import org.junit.Test;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UdpEndpointTest {


    @Test
    public void testClientServerMode() throws Exception {
        final int numPackets = 50 + new Random().nextInt(50);
        final int serverPort = PortUtil.nextServerPort();

        final BoltEndPoint server = new BoltEndPoint(InetAddress.getByName("localhost"), serverPort);
        server.start()
                .observeOn(Schedulers.computation())
                .subscribe();

        final BoltClient client = ClientUtil.runClient(serverPort,
                c -> IntStream.range(0, numPackets).forEach(__ -> c.send(TestData.getRandomData(1024))),
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
        final int serverPort = PortUtil.nextServerPort();
        final int clientPort = PortUtil.nextServerPort();
        InetAddress localhost = InetAddress.getByName("localhost");
        BoltEndPoint endpoint = new BoltEndPoint(localhost, serverPort);
        Subscription sub = endpoint.start().subscribe();
        Destination d1 = new Destination(localhost, clientPort);
        final int dataSize = BoltEndPoint.DATAGRAM_SIZE;
        final DatagramPacket p = new DatagramPacket(TestData.getRandomData(dataSize), dataSize, d1.getAddress(), d1.getPort());
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
