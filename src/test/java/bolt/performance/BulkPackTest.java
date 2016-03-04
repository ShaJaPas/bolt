package bolt.performance;

import bolt.BoltClient;
import bolt.BoltServer;
import bolt.event.ConnectionReadyEvent;
import bolt.xcoder.MessageAssembleBuffer;
import bolt.xcoder.XCoderRepository;
import org.junit.Test;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;
import java.util.concurrent.TimeoutException;

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

        final BoltClient client = new BoltClient(InetAddress.getByName("localhost"), 12345);
        client.connect(InetAddress.getByName("localhost"), 65321)
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .take(1)
                .ofType(ConnectionReadyEvent.class)
                .toBlocking()
                .subscribe(x -> {

                    try {
                        byte[] data = new byte[SIZE];
                        new Random().nextBytes(data);

                        for (int i = 0; i < PACKET_COUNT; i++)
                        {
                            client.send(data);
                            //            client.flush();
                            if (i % 10000 == 0)
                                System.out.println(i);
                        }
                        client.flush();
                        client.shutdown();
                    }
                    catch (IOException | TimeoutException | InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                });
    }

    private void runServer() throws Exception {
        final BoltServer sock = new BoltServer(XCoderRepository.create(new MessageAssembleBuffer()));

        sock.bind(InetAddress.getByName("localhost"), 65321)
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .ofType(byte[].class)
                .subscribe(x -> System.out.println("Received byte count of " + x.length),
                        ex -> { ex.printStackTrace(); fail();});
    }

}
