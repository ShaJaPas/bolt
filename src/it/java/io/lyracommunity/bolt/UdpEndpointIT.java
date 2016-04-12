package io.lyracommunity.bolt;

import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.helper.PortUtil;
import io.lyracommunity.bolt.helper.TestClient;
import io.lyracommunity.bolt.helper.TestData;
import io.lyracommunity.bolt.packet.Destination;
import io.lyracommunity.bolt.session.Session;
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

public class UdpEndpointIT
{


    @Test
    public void testClientServerMode() throws Exception {
        final int numPackets = 50 + new Random().nextInt(50);
        final int serverPort = PortUtil.nextServerPort();

        final Endpoint server = new Endpoint(InetAddress.getByName("localhost"), serverPort);
        server.start()
                .observeOn(Schedulers.computation())
                .subscribe();

        final TestClient cli = TestClient.runClient(serverPort, null,
                (tc, evt) -> IntStream.range(0, numPackets).forEach(__ -> tc.client.send(TestData.getRandomData(1024)))
        );
        cli.start(serverPort);

        // Pause until all are delivered
        boolean done = false;
        while (!done) {
            Thread.sleep(10);
            List<Session> sessions = new ArrayList<>(server.getSessions());
            Session s = sessions.isEmpty() ? null : sessions.get(0);
            done = (s != null && s.isStarted() && s.getStatistics().getNumberOfReceivedDataPackets() >= numPackets);
        }

        System.out.println(cli.client.getStatistics());
        System.out.println(server.getSessions().iterator().next().getStatistics());

        int sent = cli.client.getStatistics().getNumberOfSentDataPackets();
        int received = server.getSessions().iterator().next().getStatistics().getNumberOfReceivedDataPackets();
        assertEquals(numPackets, sent);
        assertEquals(numPackets, received);

        cli.close();
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
        final int clientPort = PortUtil.nextClientPort();
        InetAddress localhost = InetAddress.getByName("localhost");
        Endpoint endpoint = new Endpoint(localhost, serverPort);
        Subscription sub = endpoint.start().subscribe();
        Destination d1 = new Destination(localhost, clientPort);
        final int dataSize = Config.DEFAULT_DATAGRAM_SIZE;
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
        Endpoint ep = new Endpoint(InetAddress.getByName("localhost"), 0);
        int port = ep.getLocalPort();
        assertTrue(port > 0);
    }

}
