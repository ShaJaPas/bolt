package bolt.helper;

import bolt.BoltServer;
import bolt.Config;
import bolt.receiver.RoutedData;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

import java.net.InetAddress;
import java.util.function.Consumer;

/**
 * Created by keen on 24/03/16.
 */
public class ServerUtil {

    public static <T> BoltServer runServer(final Class<T> ofType, final Action1<? super T> onNext,
                                           final Action1<Throwable> onError) throws Exception {
        return runServer(ofType, onNext, onError, null);
    }

    @SuppressWarnings("unchecked")
    public static <T> BoltServer runServer(final Class<T> ofType, final Action1<? super T> onNext,
                                           final Action1<Throwable> onError, final Consumer<BoltServer> init) throws Exception {

        final BoltServer server = new BoltServer(new Config(InetAddress.getByName("localhost"), PortUtil.nextServerPort()));
        if (init != null) init.accept(server);
        TestPackets.registerAll(server.xCoderRepository());

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
