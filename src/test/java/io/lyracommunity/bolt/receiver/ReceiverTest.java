package io.lyracommunity.bolt.receiver;

import io.lyracommunity.bolt.BoltCongestionControl;
import io.lyracommunity.bolt.ChannelOut;
import io.lyracommunity.bolt.ChannelOutStub;
import io.lyracommunity.bolt.CongestionControl;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.helper.PortUtil;
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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by omahoc9 on 4/5/16.
 */
public class ReceiverTest
{

    private Config          config;
    private Receiver        sut;
    private List<Object>    events;
    private List<Throwable> errors;
    private AtomicBoolean   completed;
    private Subscription    subscription;
    private ChannelOut      endpoint;

    @Before
    public void setUp() throws Exception
    {
        setUp(null, null);
    }

    private void setUp(Long maybeExpTimerInterval, final Double initialCongestionWindowSize) throws IOException {
        config = new Config(InetAddress.getByName("localhost"), PortUtil.nextClientPort());
        if (maybeExpTimerInterval != null) config.setExpTimerInterval(maybeExpTimerInterval);
        if (initialCongestionWindowSize != null) config.setInitialCongestionWindowSize(initialCongestionWindowSize);

        endpoint = new ChannelOutStub(config, true);
        final Destination peer = new Destination(InetAddress.getByName("localhost"), PortUtil.nextServerPort());
        final Session session = new ServerSession(config, endpoint, peer);
        final SessionState sessionState = new SessionState(peer);
        sessionState.setStatus(SessionStatus.READY);
        sessionState.setActive(true);

        final CongestionControl cc = new BoltCongestionControl(sessionState, session.getStatistics(), config.getInitialCongestionWindowSize());
        final Sender sender = new Sender(config, sessionState, endpoint, cc, session.getStatistics());
        sut = new Receiver(config, sessionState, endpoint, sender, session.getStatistics());
        events = new ArrayList<>();
        errors = new ArrayList<>();
        completed = new AtomicBoolean(false);
        subscription = sut.start("Server").subscribeOn(Schedulers.computation()).observeOn(Schedulers.computation())
                .subscribe(events::add, errors::add, () -> completed.set(true));
    }

    @After
    public void tearDown() throws Exception
    {
        if (subscription != null) subscription.unsubscribe();
    }

    @Test
    public void testExpiry() throws Exception
    {
        config.setExpLimit(2);
        config.setExpTimerInterval(1);

        for (int i = 0; i < 100 && errors.isEmpty(); i++) Thread.sleep(5);
        assertFalse(errors.isEmpty());
    }

    @Test
    public void testReceiveAndProcessKeepAlive() throws Exception
    {
        config.setExpLimit(2);
        config.setExpTimerInterval(10_000);

        for (int i = 0; i < 100 && errors.isEmpty(); i++) {
            sut.receive(new KeepAlive());
            Thread.sleep(2);
        }
        assertTrue(errors.isEmpty());
    }

    @Test
    public void testReceiveAndProcessNak() throws Exception
    {
//        sut.receive();
        // TODO implement test
    }

    @Test
    public void testReceiveAndProcessData() throws Exception
    {
        // TODO implement test
    }

    @Test
    public void testReceiveAndProcessShutdown() throws Exception
    {
        // TODO implement test
    }

    @Test
    public void testReceiveAndProcessAck2() throws Exception
    {
        // TODO implement test
    }

    @Test
    public void testReceiveAndIgnoreNonReceiverControlPacket() throws Exception
    {
        // TODO implement test
    }

    @Test
    public void testCheckACKTimer() throws Exception
    {
        // TODO implement test
    }

    @Test
    public void testCheckNACKTimer() throws Exception
    {
        // TODO implement test
    }

    @Test
    public void testCheckEXPTimer() throws Exception
    {
        config.setExpTimerInterval(1);
        config.setExpLimit(2);

        for (int i = 0; i < 100 && !subscription.isUnsubscribed() && errors.isEmpty(); i++) {
            Thread.sleep(50);
        }

        assertFalse(errors.isEmpty());
        Assert.assertEquals(IllegalStateException.class, errors.get(0).getClass());
    }

    @Test
    public void testSessionExpired() throws Exception
    {
        // TODO implement test
    }

    @Test
    public void testSendNACKEvent() throws Exception
    {
        // TODO implement test
    }

    @Test
    public void testReceive() throws Exception
    {
        // TODO implement test
    }

    @Test
    public void testResetEXPTimer() throws Exception
    {
        // TODO implement test
    }

}