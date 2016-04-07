package io.lyracommunity.bolt.helper;

import io.lyracommunity.bolt.BoltClient;
import io.lyracommunity.bolt.api.Config;
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
public class TestClient implements AutoCloseable {

    public final  BoltClient                client;
    public final  Subscription              subscription;
    private final TestServer.PacketReceiver receivedByType;
    private final List<Throwable>           errors;

    public TestClient(BoltClient client, Subscription subscription, TestServer.PacketReceiver receivedByType, List<Throwable> errors) {
        this.client = client;
        this.subscription = subscription;
        this.receivedByType = receivedByType;
        this.errors = errors;
    }

    public TestClient printStatistics() {
        System.out.println(client.getStatistics());
        return this;
    }

    public List<Throwable> getErrors()
    {
        return errors;
    }

    public int getTotalReceived(final Class clazz) {
        return receivedByType.getTotalReceived(clazz);
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

        final TestServer.PacketReceiver packetReceiver = new TestServer.PacketReceiver();
        final List<Throwable> errors = new ArrayList<>();

        final Subscription subscription = client.connect(InetAddress.getByName("localhost"), serverPort)
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .subscribe(
                        x -> {
                            packetReceiver.receive(x);
                            if (onReady != null && x instanceof ConnectionReady) onReady.call(client);
                        },
                        ex -> {
                            errors.add(ex);
                            if (onError != null) onError.call(ex);
                        });

        return new TestClient(client, subscription, packetReceiver, errors);
    }

    public static List<TestClient> runClients(final int clientCount, final int serverPort, final Action1<BoltClient> onReady,
            final Action1<Throwable> onError, final Consumer<BoltClient> init) throws Exception {

        final List<TestClient> clients = new ArrayList<>(clientCount);
        for (int i = 0; i < clientCount; i++) {
            clients.add(runClient(serverPort, onReady, onError, init));
        }
        return clients;
    }

    @Override
    public void close() throws Exception
    {
        printStatistics();
        subscription.unsubscribe();
    }

}
