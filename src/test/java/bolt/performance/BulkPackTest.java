package bolt.performance;

import bolt.BoltClient;
import bolt.BoltServerSocket;
import bolt.BoltSocket;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Created by keen on 26/02/16.
 */
public class BulkPackTest {

    private static long PACKET_COUNT = 1_000_000;
    private static int SIZE = 1400;

    public static void main(String[] args) throws Exception {
        new BulkPackTest().testBulkPackets();
    }

    @Test
    public void testBulkPackets() throws Exception {

        runServer();
        runClient();
    }

    private void runClient() throws Exception {

        BoltClient client = new BoltClient(InetAddress.getByName("localhost"), 12345);
        client.connect(InetAddress.getByName("localhost"), 65321);

        long N = PACKET_COUNT * SIZE;

        byte[] data = new byte[SIZE];
        new Random().nextBytes(data);

        Thread.sleep(100);

        for (int i = 0; i < PACKET_COUNT; i++) {
            client.send(data);
//            client.flush();
            if (i % 10000 == 0) System.out.println(i);
        }
        client.flush();
        client.shutdown();
    }

    private void runServer() throws Exception {
        final BoltServerSocket sock = new BoltServerSocket(InetAddress.getByName("localhost"), 65321);

        CompletableFuture.runAsync(() -> {
            try {
                final BoltSocket s = sock.accept();
                assertNotNull(s);
                BoltInputStream is = s.getInputStream();
                byte[] buf = new byte[4096];
                int c = 0;
                while (true) {
                    c = is.read(buf);
                    if (c < 0) break;
                }
                System.out.println("Server thread exiting, last received bytes: " + c);
                sock.shutDown();
                System.out.println(s.getSession().getStatistics());
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        });
    }

}
