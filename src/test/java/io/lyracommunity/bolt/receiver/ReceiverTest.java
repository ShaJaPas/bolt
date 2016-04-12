package io.lyracommunity.bolt.receiver;

import io.lyracommunity.bolt.BoltCongestionControl;
import io.lyracommunity.bolt.CongestionControl;
import io.lyracommunity.bolt.Endpoint;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.packet.Destination;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by omahoc9 on 4/5/16.
 */
public class ReceiverTest
{

    private static final AtomicInteger CLIENT_PORT = new AtomicInteger(12045);
    private static final AtomicInteger SERVER_PORT = new AtomicInteger(60321);
    private Config config;
    private Receiver        receiver;
    private List<Object>    events;
    private List<Throwable> errors;
    private AtomicBoolean   completed;
    private Subscription    subscription;

    @Before
    public void setUp() throws Exception
    {
        setUp(null);
    }

    private void setUp(Long maybeExpTimerInterval) throws IOException {
        config = new Config(InetAddress.getByName("localhost"), CLIENT_PORT.getAndIncrement());
        if (maybeExpTimerInterval != null) config.setExpTimerInterval(maybeExpTimerInterval);

        final Endpoint endpoint = new Endpoint(config);
        final Destination peer = new Destination(InetAddress.getByName("localhost"), SERVER_PORT.getAndIncrement());
        final Session session = new ServerSession(config, endpoint, peer);
        final SessionState sessionState = new SessionState(peer);
        sessionState.setStatus(SessionStatus.READY);
        sessionState.setActive(true);

        final CongestionControl cc = new BoltCongestionControl(sessionState, session.getStatistics());
        final Sender sender = new Sender(config, sessionState, endpoint, cc, session.getStatistics());
        receiver = new Receiver(config, sessionState, endpoint, sender, session.getStatistics());
        events = new ArrayList<>();
        errors = new ArrayList<>();
        completed = new AtomicBoolean(false);
        subscription = receiver.start("ServerSession").subscribeOn(Schedulers.io()).observeOn(Schedulers.computation())
                .subscribe(events::add, errors::add, () -> completed.set(true));
    }

    @After
    public void tearDown() throws Exception
    {
        if (subscription != null) subscription.unsubscribe();
    }

    @Test
    public void testReceiveAndProcessAck() throws Exception
    {
        // TODO implement test
//        final Ack ack = Ack.buildAcknowledgement(1, 1, 10_000, 5_000, 1000, 1, 1000, 1000);
//        receiver.receive(ack);


    }

    @Test
    public void testReceiveAndProcessKeepAlive() throws Exception
    {
        // TODO implement test
    }

    @Test
    public void testReceiveAndProcessNack() throws Exception
    {
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

        Assert.assertFalse(errors.isEmpty());
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