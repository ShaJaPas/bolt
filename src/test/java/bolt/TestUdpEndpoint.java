package bolt;

import bolt.event.ConnectionReadyEvent;
import bolt.packets.Destination;
import org.junit.Test;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestUdpEndpoint extends BoltTestBase {

    @Test
    public void testClientServerMode() throws Exception {
        Logger.getLogger("bolt").setLevel(Level.INFO);
        final int numPackets = 50 + new Random().nextInt(50);
        final CountDownLatch ready = new CountDownLatch(1);

        final BoltEndPoint server = new BoltEndPoint(InetAddress.getByName("localhost"), 65322);
        final Subscription endpointSub = server.start()
                .observeOn(Schedulers.computation())
                .subscribe();

        BoltClient client = new BoltClient(InetAddress.getByName("localhost"), 12346);
        final Subscription clientSub = client.connect(InetAddress.getByName("localhost"), 65322)
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .ofType(ConnectionReadyEvent.class)
                .subscribe(__ -> ready.countDown());

        if (!ready.await(2, TimeUnit.SECONDS)) {
            throw new TimeoutException("Couldn't connect.");
        }

        // Send packets
        IntStream.range(0, numPackets).forEach(__ -> client.send((Object) getRandomData(1024)));

        // Pause until all are delivered
        List<BoltSession> sessions = new ArrayList<>(server.getSessions());
        while (sessions.get(0).getSocket().getReceiveBuffer().getNumChunks() < numPackets) Thread.sleep(10);

        Arrays.asList(clientSub, endpointSub).forEach(Subscription::unsubscribe);
        System.out.println(client.getStatistics());
        System.out.println(server.getSessions().iterator().next().getStatistics());

        int sent = client.getStatistics().getNumberOfSentDataPackets();
        int received = server.getSessions().iterator().next().getStatistics().getNumberOfReceivedDataPackets();
        assertEquals(numPackets, sent);
        assertEquals(numPackets, received);
    }


    /**
     * just check how fast we can send out UDP packets from the endpoint
     *
     * @throws Exception
     */
    @Test
    public void testRawSendRate() throws Exception {
        Logger.getLogger("bolt").setLevel(Level.WARNING);
        System.out.println("Checking raw UDP send rate...");
        InetAddress localhost = InetAddress.getByName("localhost");
        BoltEndPoint endpoint = new BoltEndPoint(localhost, 65322);
        Subscription sub = endpoint.start().subscribe();
        Destination d1 = new Destination(localhost, 12345);
        int dataSize = BoltEndPoint.DATAGRAM_SIZE;
        DatagramPacket p = new DatagramPacket(getRandomData(dataSize), dataSize, d1.getAddress(), d1.getPort());
        int N = 100_000;

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

    //@Test()
    public void testRendezvousConnect() throws Exception {

    }

    @Test
    public void testBindToAnyPort() throws Exception {
        BoltEndPoint ep = new BoltEndPoint(InetAddress.getByName("localhost"));
        int port = ep.getLocalPort();
        assertTrue(port > 0);
    }

}
