package bolt.util;

import bolt.BoltClient;
import bolt.Config;
import bolt.event.ConnectionReadyEvent;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

import java.net.InetAddress;
import java.util.function.Consumer;

/**
 * Created by keen on 24/03/16.
 */
public class ClientUtil {

    public static BoltClient runClient(final int serverPort, final Action1<BoltClient> onReady,
                                       final Action1<Throwable> onError) throws Exception {
        return runClient(serverPort, onReady, onError, null);
    }

    public static BoltClient runClient(final int serverPort, final Action1<BoltClient> onReady,
                                       final Action1<Throwable> onError, final Consumer<BoltClient> init) throws Exception {
        final Config clientConfig = new Config(InetAddress.getByName("localhost"), PortUtil.nextClientPort());
        final BoltClient client = new BoltClient(clientConfig);
        if (init != null) init.accept(client);

        client.connect(InetAddress.getByName("localhost"), serverPort)
                .subscribeOn(Schedulers.io())
                .ofType(ConnectionReadyEvent.class)
                .observeOn(Schedulers.computation())
                .subscribe(__ -> onReady.call(client), onError);

        return client;
    }
}
