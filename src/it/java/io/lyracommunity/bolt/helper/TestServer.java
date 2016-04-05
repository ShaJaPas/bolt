package io.lyracommunity.bolt.helper;

import io.lyracommunity.bolt.BoltServer;
import io.lyracommunity.bolt.Config;
import io.lyracommunity.bolt.event.ReceiveObject;
import rx.Subscription;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

import java.net.InetAddress;
import java.util.function.Consumer;

/**
 * Created by keen on 24/03/16.
 */
public class TestServer {

    public final BoltServer server;
    private final Subscription subscription;

    private TestServer(BoltServer server, Subscription subscription) {
        this.server = server;
        this.subscription = subscription;
    }

    public TestServer printStatistics() {
        server.getStatistics().forEach(System.out::println);
        return this;
    }

    public void cleanup() {
        subscription.unsubscribe();
    }

    public static <T> TestServer runObjectServer(final Class<T> ofType, final Action1<? super ReceiveObject<T>> onNext,
                                           final Action1<Throwable> onError) throws Exception {
        return runObjectServer(ofType, onNext, onError, null);
    }

    @SuppressWarnings("unchecked")
    public static <T> TestServer runObjectServer(final Class<T> ofType, final Action1<? super ReceiveObject<T>> onNext,
                                           final Action1<Throwable> onError, final Consumer<BoltServer> init) throws Exception {

        final Action1<? super Object> act = (x) -> {
            if (x instanceof ReceiveObject) {
                final ReceiveObject ro = (ReceiveObject) x;
                if (ro.isOfSubType(ofType)) onNext.call((ReceiveObject<T>)ro);
            }
        };

        return runCustomServer(act, onError, init);
    }


    @SuppressWarnings("unchecked")
    public static TestServer runCustomServer(final Action1<? super Object> onNext,
            final Action1<Throwable> onError, final Consumer<BoltServer> init) throws Exception {

        final BoltServer server = new BoltServer(new Config(InetAddress.getByName("localhost"), PortUtil.nextServerPort()));
        if (init != null) init.accept(server);
        TestObjects.registerAll(server.codecs());

        final Subscription subscription = server.bind()
                .subscribeOn(Schedulers.io())
                .onBackpressureBuffer()
                .observeOn(Schedulers.computation())
                .subscribe(onNext, onError);

        return new TestServer(server, subscription);
    }
}
