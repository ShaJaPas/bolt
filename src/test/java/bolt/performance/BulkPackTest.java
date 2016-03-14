package bolt.performance;

import bolt.BoltClient;
import bolt.BoltServer;
import bolt.Config;
import bolt.event.ConnectionReadyEvent;
import bolt.receiver.RoutedData;
import org.junit.Test;
import rx.Subscription;
import rx.observables.ConnectableObservable;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

/**
 * Created by keen on 26/02/16.
 */
public class BulkPackTest {

    public static final int SERVER_PORT = 65319;
    private static long PACKET_COUNT = 1_000_000;
    private static int SIZE = 1388;


    @Test
    public void testBulkPackets() throws Exception {
        runServer();
        runClient();
    }

    private void runClient() throws Exception {
        final CountDownLatch clientReady = new CountDownLatch(1);
        final BoltClient client = new BoltClient(InetAddress.getByName("localhost"), 12345);
        final ConnectableObservable<?> o = client.connect(InetAddress.getByName("localhost"), SERVER_PORT)
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .publish();

        Subscription clientSub = o.subscribe();

        o.ofType(ConnectionReadyEvent.class)
                .take(1)
                .subscribe(x -> {
                    try {
                        byte[] data = new byte[SIZE];
                        new Random().nextBytes(data);

                        for (int i = 0; i < PACKET_COUNT; i++) {
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
                    finally {
                        clientReady.countDown();
                    }
                });

        o.connect();
        clientReady.await();
        clientSub.unsubscribe();
    }

    private void runServer() throws Exception {
        final BoltServer server = new BoltServer(new Config(InetAddress.getByName("localhost"), SERVER_PORT));
        final AtomicInteger received = new AtomicInteger(0);
        server.bind()
                .subscribeOn(Schedulers.io())
                .onBackpressureBuffer()
                .observeOn(Schedulers.computation())
                .ofType(RoutedData.class)
                .subscribe(x -> {
                            if (received.incrementAndGet() % 10_000 == 0) System.out.println("Received " + received.get());
                        },
                        ex -> {
                            ex.printStackTrace();
                            fail();
                        });
    }

}
