package io.lyracommunity.bolt.sender;

import io.lyracommunity.bolt.BoltCongestionControl;
import io.lyracommunity.bolt.ChannelOutStub;
import io.lyracommunity.bolt.CongestionControl;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.packet.*;
import io.lyracommunity.bolt.session.SessionState;
import io.lyracommunity.bolt.session.SessionStatus;
import io.lyracommunity.bolt.statistic.BoltStatistics;
import org.junit.Before;
import org.junit.Test;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by omahoc9 on 4/5/16.
 */
public class SenderTest {

    private Sender sut;

    private SenderLossList senderLossList;

    private ChannelOutStub endpoint;

    private SessionState sessionState;

    @Before
    public void setUp() throws Exception {
        senderLossList = new SenderLossList();
        final Destination remote = new Destination(InetAddress.getLocalHost(), 65432);
        sessionState = new SessionState(remote);
        final Config config = new Config(InetAddress.getByName("localhost"), 12345);
        endpoint = new ChannelOutStub(config, true);
        final BoltStatistics statistics = new BoltStatistics("testStatistics", Config.DEFAULT_DATAGRAM_SIZE);
        final CongestionControl cc = new BoltCongestionControl(sessionState, statistics);
        sut = new Sender(config, sessionState, endpoint, cc, statistics, senderLossList);
    }

    @Test
    public void receiveAck_sendAck2InResponse() throws Exception {
        final Ack ack = Ack.buildAcknowledgement(1, 1, 10, 5, 1000, 1, 100, 100);

        sut.receive(ack);

        assertEquals(1, endpoint.sendCountOfType(PacketType.ACK2));
    }

    @Test (expected = IOException.class)
    public void receiveAck_closedEndpointCausesError() throws Exception {
        final Ack ack = Ack.buildAcknowledgement(1, 1, 10, 5, 1000, 1, 100, 100);
        endpoint.setOpen(false);

        sut.receive(ack);
    }

    @Test
    public void receiveNak() throws Exception {
        final int lossCount = 100;
        final Nak nak = new Nak();
        nak.addLossInfo(IntStream.range(0, lossCount).boxed().collect(Collectors.toList()));

        sut.receive(nak);

        assertEquals(lossCount, senderLossList.size());
        assertTrue(sut.haveLostPackets());
    }

    @Test
    public void ExpEvent_HaveUnacknowledgedPackets_AddedToLossList() throws Exception {
        final DataPacket dp = new DataPacket();
        dp.setDelivery(DeliveryType.RELIABLE_ORDERED);
        sessionState.setStatus(SessionStatus.READY);

        sut.start();
        final Subscription sub = sut.doStart("Test").subscribeOn(Schedulers.io()).subscribe();
        try {
            sut.sendPacket(dp, 10, TimeUnit.MILLISECONDS);

            for (int i = 0; i < 100; i++) {
                if (endpoint.sendCountOfType(PacketType.DATA) > 0) break;
                Thread.sleep(20);
            }

            sut.putUnacknowledgedPacketsIntoLossList();

            assertEquals(1, senderLossList.size());
        }
        finally {
            sub.unsubscribe();
        }
    }

    @Test
    public void testDoStart() throws Exception {
        // TODO implement test
    }

    @Test
    public void testSendPacket() throws Exception {
        // TODO implement test
    }

    @Test
    public void testReceive() throws Exception {
        // TODO implement test
    }

    @Test
    public void testSendAck2() throws Exception {
        // TODO implement test
    }

    @Test
    public void testHandleRetransmit() throws Exception {
        // TODO implement test
    }

    @Test
    public void testPutUnacknowledgedPacketsIntoLossList() throws Exception {
        // TODO implement test
    }

    @Test
    public void testNextPacketSequenceNumber() throws Exception {
        // TODO implement test
    }

    @Test
    public void testNextReliabilitySequenceNumber() throws Exception {
        // TODO implement test
    }

    @Test
    public void testNextOrderSequenceNumber() throws Exception {
        // TODO implement test
    }

    @Test
    public void testHaveAcknowledgementFor() throws Exception {
        // TODO implement test
    }

    @Test
    public void testIsSentOut() throws Exception {
        // TODO implement test
    }

    @Test
    public void testHaveLostPackets() throws Exception {
        // TODO implement test
    }

    @Test
    public void testWaitForAck() throws Exception {
        // TODO implement test
    }

}