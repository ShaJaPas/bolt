package io.lyracommunity.bolt.receiver;

import io.lyracommunity.bolt.BoltEndPoint;
import io.lyracommunity.bolt.Config;
import io.lyracommunity.bolt.packet.Ack;
import io.lyracommunity.bolt.packet.Destination;
import io.lyracommunity.bolt.session.BoltSession;
import io.lyracommunity.bolt.session.ServerSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Subscription;
import rx.schedulers.Schedulers;

import javax.xml.ws.Endpoint;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * Created by omahoc9 on 4/5/16.
 */
public class BoltReceiverTest
{

    private BoltReceiver receiver;
    private List<Object> events;
    private List<Throwable> errors;
    private AtomicBoolean completed;
    private Subscription subscription;

    @Before
    public void setUp() throws Exception
    {
//        final Config config = new Config(InetAddress.getByName("localhost"), 12345);
//
//
//        final BoltEndPoint endpoint = new BoltEndPoint(config);
//        final Destination peer = new Destination(InetAddress.getByName("localhost"), 65321);
//        final BoltSession session = new ServerSession(peer, endpoint);
//        receiver = new BoltReceiver(session, endpoint, config);
//        events = new ArrayList<>();
//        errors = new ArrayList<>();
//        completed = new AtomicBoolean(false);
//        subscription = receiver.start().subscribeOn(Schedulers.io()).observeOn(Schedulers.computation())
//                .subscribe(events::add, errors::add, () -> completed.set(true));
    }

    @After
    public void tearDown() throws Exception
    {
        if (subscription != null) subscription.unsubscribe();
    }

    @Test
    public void testReceiveAndProcessAck() throws Exception
    {
//        final Ack ack = Ack.buildAcknowledgement(1, 1, 10_000, 5_000, 1000, 1, 1000, 1000);
//        receiver.receive(ack);
    }

    @Test
    public void testReceiveAndProcessKeepAlive() throws Exception
    {

    }

    @Test
    public void testReceiveAndProcessNack() throws Exception
    {

    }

    @Test
    public void testReceiveAndProcessData() throws Exception
    {

    }

    @Test
    public void testReceiveAndProcessShutdown() throws Exception
    {

    }

    @Test
    public void testReceiveAndProcessAck2() throws Exception
    {

    }

    @Test
    public void testReceiveAndIgnoreNonReceiverControlPacket() throws Exception
    {

    }

    @Test
    public void testCheckACKTimer() throws Exception
    {

    }

    @Test
    public void testCheckNACKTimer() throws Exception
    {

    }

    @Test
    public void testCheckEXPTimer() throws Exception
    {

    }

    @Test
    public void testSessionExpired() throws Exception
    {

    }

    @Test
    public void testSendNACKEvent() throws Exception
    {

    }

    @Test
    public void testReceive() throws Exception
    {

    }

    @Test
    public void testResetEXPTimer() throws Exception
    {

    }

}