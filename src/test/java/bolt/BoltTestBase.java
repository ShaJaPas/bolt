package bolt;

import bolt.event.ConnectionReadyEvent;
import bolt.receiver.RoutedData;
import bolt.util.PortUtil;
import bolt.util.TestUtil;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

import java.net.InetAddress;
import java.security.MessageDigest;
import java.util.Random;

/**
 * some additional utilities useful for testing
 */
public abstract class BoltTestBase {

    public static String hexString(MessageDigest digest) {
        return TestUtil.hexString(digest);
    }

    protected int nextServerPort() {
        return PortUtil.nextServerPort();
    }

    protected int nextClientPort() {
        return PortUtil.nextClientPort();
    }

    //get an array filled with random data
    protected byte[] getRandomData(int size) {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return data;
    }

    //compute the md5 hash
    protected String computeMD5(byte[]... datablocks) throws Exception {
        MessageDigest d = MessageDigest.getInstance("MD5");
        for (byte[] data : datablocks) {
            d.update(data);
        }
        return hexString(d);
    }

    protected BoltClient runClient(final int serverPort, final Action1<BoltClient> onReady,
                                   final Action1<Throwable> onError) throws Exception {
        final Config clientConfig = new Config(InetAddress.getByName("localhost"), nextClientPort());
        final BoltClient client = new BoltClient(clientConfig);

        client.connect(InetAddress.getByName("localhost"), serverPort)
                .subscribeOn(Schedulers.io())
                .ofType(ConnectionReadyEvent.class)
                .observeOn(Schedulers.computation())
                .subscribe(__ -> onReady.call(client), onError);

        return client;
    }

    @SuppressWarnings("unchecked")
    protected <T> BoltServer runServer(final Class<T> ofType, final Action1<? super T> onNext,
                                       final Action1<Throwable> onError) throws Exception {

        final BoltServer server = new BoltServer(new Config(InetAddress.getByName("localhost"), nextServerPort()));

        server.bind()
                .subscribeOn(Schedulers.io())
                .onBackpressureBuffer()
                .observeOn(Schedulers.computation())
                .ofType(RoutedData.class)
                .filter(rd -> ofType.isAssignableFrom(rd.getPayload().getClass()))
                .map(rd -> (T) rd.getPayload())
                .subscribe(onNext, onError);

        return server;
    }


}
