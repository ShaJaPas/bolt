package io.lyracommunity.bolt.receiver;

import io.lyracommunity.bolt.BoltCongestionControl;
import io.lyracommunity.bolt.BoltEndPoint;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.CongestionControl;
import io.lyracommunity.bolt.packet.Destination;
import io.lyracommunity.bolt.sender.Sender;
import io.lyracommunity.bolt.session.Session;
import io.lyracommunity.bolt.session.ServerSession;
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

/**
 * Created by omahoc9 on 4/5/16.
 */
public class ReceiverTest
{

    private Config          config;
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
        config = new Config(InetAddress.getByName("localhost"), 12345);
        if (maybeExpTimerInterval != null) config.setExpTimerInterval(maybeExpTimerInterval);

        final BoltEndPoint endpoint = new BoltEndPoint(config);
        final Destination peer = new Destination(InetAddress.getByName("localhost"), 65321);
        final Session session = new ServerSession(config, endpoint, peer);
        final SessionState sessionState = new SessionState(peer, "Server");
        final CongestionControl cc = new BoltCongestionControl(sessionState);
        final Sender sender = new Sender(config, sessionState, endpoint, cc);
        session.setStatus(SessionStatus.READY);
        receiver = new Receiver(config, sessionState, endpoint, sender);
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

        for (int i = 0; i < 10 && !subscription.isUnsubscribed() && errors.isEmpty(); i++) {
            Thread.sleep(500);
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