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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

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
        setUp(null);
    }

    private void setUp(Double initialCongestionWindowSize) throws UnknownHostException {
        senderLossList = new SenderLossList();
        final Destination remote = new Destination(InetAddress.getLocalHost(), 65432);
        sessionState = new SessionState(remote);
        final Config config = new Config(InetAddress.getByName("localhost"), 12345);
        if (initialCongestionWindowSize != null) config.setInitialCongestionWindowSize(initialCongestionWindowSize);
        endpoint = new ChannelOutStub(config, true);
        final BoltStatistics statistics = new BoltStatistics("testStatistics", Config.DEFAULT_DATAGRAM_SIZE);
        final CongestionControl cc = new BoltCongestionControl(sessionState, statistics, config.getInitialCongestionWindowSize());
        sut = new Sender(config, sessionState, endpoint, cc, statistics, senderLossList);
    }

    @Test
    public void receiveAck_sendAck2InResponse() throws Exception {
        final Ack ack = Ack.buildAcknowledgement(1, 1, 10, 5, 1000, 1, 100, 100);

        sut.receive(ack);

        assertEquals(1, endpoint.sendCountOfType(PacketType.ACK2));
    }

    @Test(expected = IOException.class)
    public void receiveAck_closedEndpointCausesError() throws Exception {
        final Ack ack = Ack.buildAcknowledgement(1, 1, 10, 5, 1000, 1, 100, 100);
        endpoint.setOpen(false);

        sut.receive(ack);
    }

    @Test
    public void receiveNak_LossListPopulated() throws Exception {
        final int lossCount = 100;
        final Nak nak = new Nak();
        nak.addLossList(IntStream.range(0, lossCount).boxed().collect(Collectors.toList()));

        sut.receive(nak);

        assertEquals(lossCount, senderLossList.size());
        assertTrue(sut.haveLostPackets());
    }

    @Test
    public void ExpEvent_HaveUnacknowledgedPackets_AddedToLossList() throws Exception {
        final DataPacket dp = new DataPacket();
        dp.setDelivery(DeliveryType.RELIABLE_ORDERED);
        sessionState.setStatus(SessionStatus.READY);

        sut.sendPacket(dp);

        sut.senderAlgorithm();

        sut.putUnacknowledgedPacketsIntoLossList();

        assertEquals(1, senderLossList.size());
    }

    @Test
    public void retransmit() throws Exception {
        // Given
        sessionState.setStatus(SessionStatus.READY);
        final DataPacket dp = new DataPacket();
        dp.setReliabilitySeqNumber(1);
        dp.setDelivery(DeliveryType.RELIABLE_UNORDERED);
        final Nak nak = new Nak();
        nak.addLossSingle(1);

        // When
        sut.sendPacket(dp);
        // Now wait until send has occurred.
        sut.senderAlgorithm();
        sut.receive(nak);
        sut.senderAlgorithm();

        // Then
        assertEquals(2, endpoint.sendCountOfType(PacketType.DATA));
        assertFalse(sut.haveLostPackets());
    }

    @Test
    public void testIsSentOut() throws Exception {
        // Given
        sessionState.setStatus(SessionStatus.READY);
        final DataPacket dp = new DataPacket();
        dp.setReliabilitySeqNumber(1);
        dp.setDelivery(DeliveryType.RELIABLE_UNORDERED);

        // When
        sut.sendPacket(dp);
        sut.senderAlgorithm();

        // Then
        assertTrue(sut.isSentOut(sessionState.getInitialSequenceNumber()));
    }

}