package bolt;

import bolt.event.ConnectionReadyEvent;
import bolt.receiver.RoutedData;
import bolt.util.PortUtil;
import bolt.util.TestUtil;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

import java.net.InetAddress;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * some additional utilities useful for testing
 */
public abstract class BoltTestBase {

    protected final Set<Throwable> errors = new HashSet<>();
    protected final AtomicInteger received = new AtomicInteger(0);

    public static String hexString(MessageDigest digest) {
        return TestUtil.hexString(digest);
    }

    protected int nextServerPort() {
        return PortUtil.nextServerPort();
    }

    protected int nextClientPort() {
        return PortUtil.nextClientPort();
    }

    // Get an array filled with random data
    protected byte[] getRandomData(final int size) {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return data;
    }

    // Get an array filled with seeded random data
    protected byte[] getRandomData(final int seed, final int size) {
        byte[] data = new byte[size];
        new Random(seed).nextBytes(data);
        return data;
    }

    // Compute the md5 hash
    protected String computeMD5(byte[]... datablocks) throws Exception {
        MessageDigest d = MessageDigest.getInstance("MD5");
        for (byte[] data : datablocks) {
            d.update(data);
        }
        return hexString(d);
    }

    protected BoltClient runClient(final int serverPort, final Action1<BoltClient> onReady,
                                   final Action1<Throwable> onError) throws Exception {
        return runClient(serverPort, onReady, onError, null);
    }

    protected BoltClient runClient(final int serverPort, final Action1<BoltClient> onReady,
                                   final Action1<Throwable> onError, final Consumer<BoltClient> init) throws Exception {
        final Config clientConfig = new Config(InetAddress.getByName("localhost"), nextClientPort());
        final BoltClient client = new BoltClient(clientConfig);
        if (init != null) init.accept(client);

        client.connect(InetAddress.getByName("localhost"), serverPort)
                .subscribeOn(Schedulers.io())
                .ofType(ConnectionReadyEvent.class)
                .observeOn(Schedulers.computation())
                .subscribe(__ -> onReady.call(client), onError);

        return client;
    }

    protected <T> BoltServer runServer(final Class<T> ofType, final Action1<? super T> onNext,
                                       final Action1<Throwable> onError) throws Exception {
        return runServer(ofType, onNext, onError, null);
    }

    @SuppressWarnings("unchecked")
    protected <T> BoltServer runServer(final Class<T> ofType, final Action1<? super T> onNext,
                                       final Action1<Throwable> onError, final Consumer<BoltServer> init) throws Exception {

        final BoltServer server = new BoltServer(new Config(InetAddress.getByName("localhost"), nextServerPort()));
        if (init != null) init.accept(server);

        server.bind()
                .subscribeOn(Schedulers.io())
                .onBackpressureBuffer()
                .observeOn(Schedulers.computation())
                .ofType(RoutedData.class)
                .filter(rd -> rd.isOfSubType(ofType))
                .map(rd -> (T) rd.getPayload())
                .subscribe(onNext, onError);

        return server;
    }


}
