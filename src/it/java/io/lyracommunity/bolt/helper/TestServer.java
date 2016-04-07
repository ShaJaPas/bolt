package io.lyracommunity.bolt.helper;

import io.lyracommunity.bolt.BoltServer;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.event.ReceiveObject;
import rx.Subscription;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Created by keen on 24/03/16.
 */
public class TestServer implements AutoCloseable {

    public final  BoltServer                server;
    private final Subscription              subscription;
    private final PacketReceiver receivedByType;
    private final List<Throwable>           errors;

    private TestServer(BoltServer server, Subscription subscription, PacketReceiver receivedByType, List<Throwable> errors) {
        this.server = server;
        this.subscription = subscription;
        this.receivedByType = receivedByType;
        this.errors = errors;
    }

    public TestServer printStatistics() {
        server.getStatistics().forEach(System.out::println);
        return this;
    }

    public int getTotalReceived(final Class clazz) {
        return receivedByType.getTotalReceived(clazz);
    }

    public List<Throwable> getErrors()
    {
        return errors;
    }

    public static <T> TestServer runObjectServer(final Class<T> ofType, final Action1<? super ReceiveObject<T>> onNext,
                                           final Action1<Throwable> onError) throws Exception {
        return runObjectServer(ofType, onNext, onError, null);
    }

    @SuppressWarnings("unchecked")
    public static <T> TestServer runObjectServer(final Class<T> ofType, final Action1<? super ReceiveObject<T>> onNext,
                                           final Action1<Throwable> onError, final Consumer<BoltServer> init) throws Exception {

        final Action1<? super Object> act = (x) -> {
            if (onNext != null && x instanceof ReceiveObject) {
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

        final List<Throwable> errors = new ArrayList<>();
        final PacketReceiver packetReceiver = new PacketReceiver();

        final Subscription subscription = server.bind()
                .subscribeOn(Schedulers.io())
                .onBackpressureBuffer()
                .observeOn(Schedulers.computation())
                .subscribe(
                        (x) -> {
                            packetReceiver.receive(x);
                            if (onNext != null) onNext.call(x);
                        },
                        (ex) -> {
                            errors.add(ex);
                            if (onError != null) onError.call(ex);
                        });

        return new TestServer(server, subscription, packetReceiver, errors);
    }

    static class PacketReceiver {

        final Map<Class, AtomicInteger> totalReceived = new HashMap<>();

        void receive(final Object o) {
            final Class clazz = o.getClass();
            AtomicInteger maybeInt = totalReceived.get(clazz);
            if (maybeInt == null) totalReceived.putIfAbsent(clazz, new AtomicInteger(0));
            totalReceived.get(clazz).incrementAndGet();

            if (clazz.equals(ReceiveObject.class)) receive(((ReceiveObject)o).getPayload());
        }

        public int getTotalReceived(final Class clazz) {
            final AtomicInteger r = totalReceived.get(clazz);
            return (r == null) ? 0 : r.get();
        }

    }

    @Override
    public void close() throws Exception
    {
        printStatistics();
        subscription.unsubscribe();
    }

}
