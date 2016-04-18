package io.lyracommunity.bolt.receiver;

import io.lyracommunity.bolt.BoltCongestionControl;
import io.lyracommunity.bolt.ChannelOut;
import io.lyracommunity.bolt.ChannelOutStub;
import io.lyracommunity.bolt.CongestionControl;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.helper.PortUtil;
import io.lyracommunity.bolt.helper.TestData;
import io.lyracommunity.bolt.packet.DataPacket;
import io.lyracommunity.bolt.packet.DeliveryType;
import io.lyracommunity.bolt.packet.Destination;
import io.lyracommunity.bolt.packet.KeepAlive;
import io.lyracommunity.bolt.sender.Sender;
import io.lyracommunity.bolt.session.ServerSession;
import io.lyracommunity.bolt.session.Session;
import io.lyracommunity.bolt.session.SessionState;
import io.lyracommunity.bolt.session.SessionStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * Created by omahoc9 on 4/5/16.
 */
public class ReceiverTest {

    private Config          config;
    private Receiver        sut;
    private List<Throwable> errors;
    private AtomicBoolean   completed;
    private Subscription    subscription;

    @Before
    public void setUp() throws Exception {
        setUp(null, null);
    }

    private void setUp(Long maybeExpTimerInterval, Double initialCongestionWindowSize) throws IOException {
        config = new Config(InetAddress.getByName("localhost"), PortUtil.nextClientPort());
        if (maybeExpTimerInterval != null) config.setExpTimerInterval(maybeExpTimerInterval);
        if (initialCongestionWindowSize != null) config.setInitialCongestionWindowSize(initialCongestionWindowSize);
        EventTimers timers = new EventTimers(config);
        ChannelOut endpoint = new ChannelOutStub(config, true);
        final Destination peer = new Destination(InetAddress.getByName("localhost"), PortUtil.nextServerPort());
        final Session session = new ServerSession(config, endpoint, peer);
        final SessionState sessionState = new SessionState(peer);
        sessionState.setStatus(SessionStatus.READY);
        sessionState.setActive(true);

        final CongestionControl cc = new BoltCongestionControl(sessionState, session.getStatistics(), config.getInitialCongestionWindowSize());
        final Sender sender = new Sender(config, sessionState, endpoint, cc, session.getStatistics());
        sut = new Receiver(config, sessionState, endpoint, sender, session.getStatistics(), timers);
        List<Object> events = new ArrayList<>();
        errors = new ArrayList<>();
        completed = new AtomicBoolean(false);
        subscription = sut.start("Server").subscribeOn(Schedulers.computation()).observeOn(Schedulers.computation())
                .subscribe(events::add, errors::add, () -> completed.set(true));
    }

    @After
    public void tearDown() throws Exception {
        if (subscription != null) subscription.unsubscribe();
    }

    @Test
    public void testExpiry() throws Exception {
        config.setExpLimit(2);
        config.setExpTimerInterval(1);

        for (int i = 0; i < 100 && errors.isEmpty(); i++) Thread.sleep(5);
        assertFalse(errors.isEmpty());
    }

    @Test
    public void testReceiveAndProcessKeepAlive() throws Exception {
        config.setExpLimit(2);
        config.setExpTimerInterval(10_000);

        for (int i = 0; i < 100 && errors.isEmpty(); i++) {
            sut.receive(new KeepAlive());
            Thread.sleep(2);
        }
        assertTrue(errors.isEmpty());
    }

    @Test
    public void testReceiveAndProcessData() throws Exception {
        sut.receive(createDataPacket(1, TestData.getRandomData(1000)));

        assertNotNull(sut.pollReceiveBuffer(1, TimeUnit.SECONDS));
    }

    @Test
    public void testCheckEXPTimer() throws Exception {
        config.setExpTimerInterval(1);
        config.setExpLimit(2);

        for (int i = 0; i < 100 && !subscription.isUnsubscribed() && errors.isEmpty(); i++) {
            Thread.sleep(50);
        }

        assertFalse(errors.isEmpty());
        Assert.assertEquals(IllegalStateException.class, errors.get(0).getClass());
    }

    private DataPacket createDataPacket(int relSeqNum, byte[] data) {
        DataPacket dp = new DataPacket();
        dp.setPacketSeqNumber(relSeqNum);
        dp.setReliabilitySeqNumber(relSeqNum);
        dp.setData(data);
        dp.setDelivery(DeliveryType.RELIABLE_UNORDERED);
        return dp;
    }

    //    @Test
    public void performanceThroughputOfReceive() throws Exception {
        final long start = System.nanoTime();
        final CountDownLatch done = new CountDownLatch(1);
        final int packetCount = 1_000_000;
        final byte[] data = TestData.getRandomData(1000);

        CompletableFuture.runAsync(() -> {
            int count = 0;
            while (count < packetCount) {
                try {
                    if (sut.pollReceiveBuffer(100, TimeUnit.MILLISECONDS) != null) {
                        if (count % 10_000 == 0) System.out.println("POLL  " + count);
                        count++;
                    }
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            done.countDown();

        });
        for (int i = 0; i < packetCount; i++) {
            sut.receive(createDataPacket(i, data));
            if (i % 1000 == 0) Thread.sleep(10);
            if (i % 10_000 == 0) System.out.println("RECV  " + i);
        }
        done.await();
        System.out.println("Took:  " + ((System.nanoTime() - start) / 1_000_000) + "ms");
    }


}