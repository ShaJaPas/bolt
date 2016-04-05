package io.lyracommunity.bolt.helper;

import io.lyracommunity.bolt.BoltClient;
import io.lyracommunity.bolt.Config;
import io.lyracommunity.bolt.event.ConnectionReady;
import rx.Subscription;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by keen on 24/03/16.
 */
public class TestClient {

    public final BoltClient client;
    public final Subscription subscription;

    public TestClient(BoltClient client, Subscription subscription) {
        this.client = client;
        this.subscription = subscription;
    }

    public TestClient printStatistics() {
        System.out.println(client.getStatistics());
        return this;
    }

    public void cleanup() {
        subscription.unsubscribe();
    }

    public static TestClient runClient(final int serverPort, final Action1<BoltClient> onReady,
                                       final Action1<Throwable> onError) throws Exception {
        return runClient(serverPort, onReady, onError, null);
    }

    public static TestClient runClient(final int serverPort, final Action1<BoltClient> onReady,
                                       final Action1<Throwable> onError, final Consumer<BoltClient> init) throws Exception {
        final Config clientConfig = new Config(InetAddress.getByName("localhost"), PortUtil.nextClientPort());
        final BoltClient client = new BoltClient(clientConfig);
        if (init != null) init.accept(client);
        TestObjects.registerAll(client.codecs());

        final Subscription subscription = client.connect(InetAddress.getByName("localhost"), serverPort)
                .subscribeOn(Schedulers.io())
                .ofType(ConnectionReady.class)
                .observeOn(Schedulers.computation())
                .subscribe(__ -> onReady.call(client), onError);

        return new TestClient(client, subscription);
    }

    public static List<TestClient> runClients(final int clientCount, final int serverPort, final Action1<BoltClient> onReady,
            final Action1<Throwable> onError, final Consumer<BoltClient> init) throws Exception {

        final List<TestClient> clients = new ArrayList<>(clientCount);
        for (int i = 0; i < clientCount; i++) {
            clients.add(runClient(serverPort, onReady, onError, init));
        }
        return clients;
    }

}
