package io.lyracommunity.bolt.helper;

import io.lyracommunity.bolt.BoltServer;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.event.ConnectionReady;
import io.lyracommunity.bolt.event.ReceiveObject;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Created by keen on 24/03/16.
 */
public class TestServer implements AutoCloseable {

    public final  BoltServer                server;
    private       Subscription              subscription;
    private final PacketReceiver            receivedByType;
    private final List<Throwable>           errors;
    private final BiConsumer<TestServer, Object> onNext;
    private final BiConsumer<TestServer, ConnectionReady> onReady;

    private TestServer(final BoltServer server, PacketReceiver receivedByType, final List<Throwable> errors,
                       final BiConsumer<TestServer, Object> onNext, final BiConsumer<TestServer, ConnectionReady> onReady) {
        this.server = server;
        this.receivedByType = receivedByType;
        this.errors = errors;
        this.onNext = onNext;
        this.onReady = onReady;
    }

    void start() {
        subscription = server.bind()
                .subscribeOn(Schedulers.io())
                .onBackpressureBuffer()
                .observeOn(Schedulers.computation())
                .subscribe(
                        (x) -> {
                            receivedByType.receive(x);
                            if (onNext != null) {
                                if (ReceiveObject.class.equals(x.getClass())) onNext.accept(this, ((ReceiveObject)x).getPayload());
                                onNext.accept(this, x);
                            }
                            if (onReady != null && ConnectionReady.class.equals(x.getClass())) onReady.accept(this, (ConnectionReady) x);
                        },
                        errors::add);
    }

    private TestServer printStatistics() {
        server.getStatistics().forEach(System.out::println);
        return this;
    }

    public int receivedOf(final Class clazz) {
        return receivedByType.getTotalReceived(clazz);
    }

    public List<Throwable> getErrors()
    {
        return errors;
    }

    @SuppressWarnings("unchecked")
    public static TestServer runCustomServer(final BiConsumer<TestServer, Object> onNext,
                                             final BiConsumer<TestServer, ConnectionReady> onReady,
                                             final Consumer<BoltServer> init) throws Exception {

        final BoltServer server = new BoltServer(new Config(InetAddress.getByName("localhost"), PortUtil.nextServerPort()));
        if (init != null) init.accept(server);
        TestObjects.registerAll(server.codecs());

        final List<Throwable> errors = new ArrayList<>();
        final PacketReceiver packetReceiver = new PacketReceiver();

        return new TestServer(server, packetReceiver, errors, onNext, onReady);
    }

    @Override
    public void close()
    {
        printStatistics();
        subscription.unsubscribe();
    }

}
