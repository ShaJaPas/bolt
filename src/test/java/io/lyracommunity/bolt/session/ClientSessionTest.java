package io.lyracommunity.bolt.session;

import io.lyracommunity.bolt.Endpoint;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.helper.PortUtil;
import io.lyracommunity.bolt.helper.TestData;
import io.lyracommunity.bolt.packet.BoltPacket;
import io.lyracommunity.bolt.packet.DataPacket;
import io.lyracommunity.bolt.packet.DeliveryType;
import io.lyracommunity.bolt.packet.Destination;
import org.junit.Before;
import org.junit.Test;
import rx.Subscription;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Created by omahoc9 on 4/5/16.
 */
public class ClientSessionTest {

    private ClientSession sut;

    private Endpoint endpoint;

    @Before
    public void setUp() throws Exception {
        final Config config = new Config(InetAddress.getLocalHost(), PortUtil.nextClientPort());
        final Destination remote = new Destination(InetAddress.getLocalHost(), PortUtil.nextServerPort());
        endpoint = new Endpoint("", config) {
            @Override
            public void doSend(BoltPacket packet, SessionState sessionState) throws IOException {
                // Do nothing
            }
        };
        sut = new ClientSession(config, endpoint, remote);
    }

    @Test
    public void connect_successReady() throws Exception {
        final CountDownLatch complete = new CountDownLatch(1);

        sut.setStatus(SessionStatus.READY);
        sut.connect().subscribeOn(Schedulers.computation()).subscribe(x -> {
        }, ex -> {
        }, complete::countDown);

        assertTrue(complete.await(200, TimeUnit.MILLISECONDS));
    }

    @Test
    public void connect_failureEndpointClosed() throws Exception {
        endpoint.stop(new TestSubscriber<>());
        final Set<Throwable> errors = new HashSet<>();
        final CountDownLatch complete = new CountDownLatch(1);

        sut.connect().subscribeOn(Schedulers.computation()).subscribe(x -> {
        }, ex -> {
            errors.add(ex);
            complete.countDown();
        }, complete::countDown);

        assertTrue(complete.await(200, TimeUnit.MILLISECONDS));
        assertFalse(errors.isEmpty());
    }

    @Test
    public void receiveData() throws Exception {
        final DataPacket received = createPacket();
        sut.setStatus(SessionStatus.READY);
        final Subscription sub = sut.start().subscribeOn(Schedulers.computation()).subscribe();

        sut.received(received, new TestSubscriber<>());

        final DataPacket polled = sut.pollReceiveBuffer(10000, TimeUnit.MILLISECONDS);

        assertEquals(received, polled);
        sub.unsubscribe();
    }

    private DataPacket createPacket() {
        final DataPacket p = new DataPacket();
        p.setReliabilitySeqNumber(1);
        p.setData(TestData.getRandomData(1000));
        p.setOrderSeqNumber(1);
        p.setDelivery(DeliveryType.RELIABLE_ORDERED);
        return p;
    }
}