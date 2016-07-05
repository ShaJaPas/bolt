package io.lyracommunity.bolt.sender;

import io.lyracommunity.bolt.ChannelOut;
import io.lyracommunity.bolt.CongestionControl;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.packet.*;
import io.lyracommunity.bolt.receiver.Receiver;
import io.lyracommunity.bolt.session.SessionState;
import io.lyracommunity.bolt.statistic.BoltStatistics;
import io.lyracommunity.bolt.util.SeqNum;
import io.lyracommunity.bolt.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Sender half of a Bolt entity.
 * <p>
 * The sender sends (and retransmits) application data according to the
 * flow control and congestion control.
 *
 * @see Receiver
 */
public class Sender {

    private static final Logger LOG = LoggerFactory.getLogger(Sender.class);

    private final ChannelOut endpoint;

    private final BoltStatistics statistics;

    /**
     * Stores the reliability seq numbers of lost packets fed back by the receiver through NAK packets
     */
    private final SenderLossList senderLossList;

    /**
     * Stores the sent data packets and their sequence numbers.
     */
    private final Map<Integer, DataPacket> sendBuffer;

    private final FlowWindow flowWindow;

    /**
     * Protects against races when reading/writing to the sendBuffer.
     * TODO consider alternatives (could be a performance bottleneck).
     */
    private final Object sendLock = new Object();

    /**
     * Number of unacknowledged data packets.
     */
    private final AtomicInteger unacknowledged = new AtomicInteger(0);

    /**
     * Used by the sender to wait for an ACK.
     */
    private final ReentrantLock ackLock      = new ReentrantLock();
    private final Condition     ackCondition = ackLock.newCondition();
    private final CongestionControl cc;
    private final SessionState      sessionState;
    /**
     * For generating data packet sequence numbers.
     */
    private volatile int currentSequenceNumber            = 0;
    /**
     * For generating reliability sequence numbers.
     */
    private volatile int currentReliabilitySequenceNumber = 0;
    /**
     * For generating order sequence numbers.
     */
    private volatile int currentOrderSequenceNumber       = 0;
    /**
     * The largest data packet sequence number that has actually been sent out.
     */
    private volatile int largestSentSequenceNumber        = -1;
    /**
     * Last acknowledge number, initialised to the initial sequence number.
     */
    private volatile int lastAckReliabilitySequenceNumber = 0;
    private volatile boolean started = false;
    private volatile long nextStep;


    public Sender(final Config config, final SessionState state, final ChannelOut endpoint, final CongestionControl cc,
                  final BoltStatistics statistics) {
        this(config, state, endpoint, cc, statistics, new SenderLossList());
    }

    Sender(final Config config, final SessionState state, final ChannelOut endpoint, final CongestionControl cc,
           final BoltStatistics statistics, final SenderLossList senderLossList) {
        this.endpoint = endpoint;
        this.cc = cc;
        this.statistics = statistics;
        this.sessionState = state;
        this.senderLossList = senderLossList;
        this.sendBuffer = new ConcurrentHashMap<>(sessionState.getFlowWindowSize(), 0.75f, 2);

        this.lastAckReliabilitySequenceNumber = 0;
        this.currentSequenceNumber = state.getInitialSequenceNumber() - 1;

        final int chunkSize = config.getDatagramSize() - 24;
        this.flowWindow = new FlowWindow(config.isMemoryPreAllocation(), sessionState.getFlowWindowSize(), chunkSize);
    }

    /**
     * Start the sender thread.
     */
    public void start() {
        LOG.info("Starting sender for {}", sessionState);
        started = true;
    }

    /**
     * Sends the given data packet, storing the relevant information.
     */
    private void send(final DataPacket dp) throws IOException {
        synchronized (sendLock) {
            statistics.beginSend();

            endpoint.doSend(dp, sessionState);

            statistics.endSend();

            if (dp.isReliable()) {
                // Store data for potential retransmit.
                final DataPacket buffered = new DataPacket();
                buffered.copyFrom(dp);
                sendBuffer.put(dp.getReliabilitySeqNumber(), buffered);
                unacknowledged.incrementAndGet();
            }
            largestSentSequenceNumber = dp.getPacketSeqNumber();
        }
        statistics.incNumberOfSentDataPackets();
    }

    /**
     * Writes a data packet, waiting at most for the specified time.
     * If this is not possible due to a full send queue.
     *
     * @param src the data packet to send.
     * @throws InterruptedException if the thread was interrupted while waiting to send.
     */
    public void sendPacket(final DataPacket src) throws InterruptedException {
        if (!started) start();
        src.setDestinationID(sessionState.getDestinationSessionID());
        src.setPacketSeqNumber(nextPacketSequenceNumber());
        src.setReliabilitySeqNumber(src.isReliable() ? nextReliabilitySequenceNumber() : 0);
        src.setOrderSeqNumber(src.isOrdered() ? nextOrderSequenceNumber() : 0);

        boolean complete = false;
        while (!complete) {
            complete = flowWindow.tryProduce(src, 100, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Receive a packet from server from the peer.
     *
     * @param received the received packet.
     */
    public void receive(final BoltPacket received) throws IOException {
        if (received.isControlPacket()) {
            if (PacketType.ACK == received.getPacketType()) {
                onAckReceived((Ack) received);
            }
            else if (PacketType.NAK == received.getPacketType()) {
                onNakReceived((Nak) received);
            }
        }
    }

    /**
     * On ACK packet received:
     * <ol>
     * <li> Update the largest acknowledged sequence number.
     * <li> Send back an ACK2 with the same ACK sequence number in this ACK.
     * <li> Update RTT and RTTVar.
     * <li> Update both ACK and NAK period to 4 * RTT + RTTVar + SYN.
     * <li> Update flow window size.
     * <li> If this is a Light ACK, stop.
     * <li> Update packet arrival rate: A = (A * 7 + a) / 8, where a is the value carried in the ACK.
     * <li> Update estimated link capacity: B = (B * 7 + b) / 8, where b is the value carried in the ACK.
     * <li> Update sender's buffer (by releasing the buffer that has been acknowledged).
     * <li> Update sender's loss list (by removing all those that has been acknowledged).
     * </ol>
     *
     * @param ack the received ACK packet.
     * @throws IOException if sending of ACK2 fails.
     */
    private void onAckReceived(final Ack ack) throws IOException {
        ackLock.lock();
        ackCondition.signal();
        ackLock.unlock();

        // TODO this method needs to perform better.
        final long rtt = ack.getRoundTripTime();
        if (rtt > 0) {
            final long rttVar = ack.getRoundTripTimeVar();
            cc.setRTT(rtt, rttVar);
            statistics.setRTT(rtt, rttVar);
        }
        final long rate = ack.getPacketReceiveRate();
        if (rate > 0) {
            long linkCapacity = ack.getEstimatedLinkCapacity();
            cc.updatePacketArrivalRate(rate, linkCapacity);
            statistics.setPacketArrivalRate(cc.getPacketArrivalRate(), cc.getEstimatedLinkCapacity());
        }

        final int ackNumber = ack.getAckNumber();
        cc.onACK(ackNumber);
        statistics.setCongestionWindowSize((long) cc.getCongestionWindowSize());
        // Need to remove all sequence numbers up the ACK number from the sendBuffer.
        boolean removed;
        for (int s = lastAckReliabilitySequenceNumber; SeqNum.compare16(s, ackNumber) < 0; s = SeqNum.increment16(s)) {
            synchronized (sendLock) {
                removed = sendBuffer.remove(s) != null;
                senderLossList.remove(s);
            }
            if (removed) {
                unacknowledged.decrementAndGet();
            }
        }
        lastAckReliabilitySequenceNumber = SeqNum.compare16(lastAckReliabilitySequenceNumber, ackNumber) > 0
                ? lastAckReliabilitySequenceNumber
                : ackNumber;
        // Send ACK2 packet to the receiver.
        sendAck2(ackNumber);
        statistics.incNumberOfACKReceived();
    }

    /**
     * On NAK packet received:
     * <ol>
     * <li> Add all sequence numbers carried in the NAK into the sender's loss list.
     * <li> Update the SND period by rate control (see section 3.6).
     * <li> Reset the EXP time variable.
     * </ol>
     *
     * @param nak NAK packet received.
     */
    private void onNakReceived(final Nak nak) {
        nak.computeExpandedLossList().forEach(senderLossList::insert);

        cc.onLoss(nak.computeExpandedLossList(), getCurrentReliabilitySequenceNumber());
        statistics.incNumberOfNAKReceived();

        if (LOG.isDebugEnabled()) {
            LOG.debug("NAK for {} packets lost, set send period to {}", cc.getSendInterval());
        }
    }

    private void sendAck2(long ackSequenceNumber) throws IOException {
        final Ack2 ackOfAckPkt = Ack2.build(ackSequenceNumber, sessionState.getDestinationSessionID());
        endpoint.doSend(ackOfAckPkt, sessionState);
    }

    /**
     * Data Sending Algorithm:
     * <ol>
     * <li> If the sender's loss list is not empty, retransmit the first
     * packet in the list and remove it from the list. Go to 5).
     * <li> In messaging mode, if the packets has been the loss list for a
     * time more than the application specified TTL (time-to-live), send
     * a message drop request and remove all related packets from the
     * loss list. Go to 1).
     * <li> Wait until there is application data to be sent.
     * <li>
     * a. If the number of unacknowledged packets exceeds the flow/congestion
     * window size, wait until an ACK comes. Go back to step 1). <br/>
     * b. Pack a new data packet and send it out.
     * </ol>
     * <li> If the sequence number of the current packet is 16n, where n is an
     * integer, go to 2).
     * <li> Wait (SND - t) time, where SND is the inter-packet interval updated by
     * congestion control and t is the total time used by step 1 to step 5. Go to 1).
     * </ol>
     *
     * @return minimum time that the next step should begin, in microseconds.
     * @throws IOException          on failure to send the DataPacket.
     * @throws InterruptedException on the thread being interrupted.
     */
    long senderAlgorithm() throws IOException, InterruptedException {
        final long stepStartTime = Util.currentTimeMicros();

        // If step or session not ready, prevent entering.
        if (stepStartTime < nextStep) return nextStep;
        if (!sessionState.isReady() || !started) return nextStep = Util.currentTimeMicros() + 5_000;

        // If the sender's loss list is not empty
        final Integer lossEntry = senderLossList.getFirstEntry();
        if (lossEntry != null) {
            handleRetransmit(lossEntry);
        }
        else {
            // If the number of unacknowledged data packets does not exceed the congestion
            // and the flow window sizes, pack a new packet.
            final int unAcknowledged = unacknowledged.get();

            if (unAcknowledged < cc.getCongestionWindowSize()
                    && unAcknowledged < sessionState.getFlowWindowSize()) {
                // Check for application data
                final DataPacket dp = flowWindow.consumeData();
                if (dp != null) {
                    send(dp);
                }
                else {
                    statistics.incNumberOfMissingDataEvents();
                }
            }
            else {
                // Congestion window full, wait for an ack
                if (unAcknowledged >= cc.getCongestionWindowSize()) {
                    statistics.incNumberOfCCWindowExceededEvents();
                }
                // TODO this may be a bottleneck for servers with many clients connected
                waitForAck();
            }
        }

        // Wait
        if (largestSentSequenceNumber % 16 != 0) {
            final long snd = (long) cc.getSendInterval();
            nextStep = snd + stepStartTime;
            if (Util.currentTimeMicros() < nextStep) {
                statistics.incNumberOfCCSlowDownEvents();
            }
        }
        return nextStep;
    }

    /**
     * Re-transmit an entry from the sender loss list.
     *
     * @param reliabilitySeqNum reliability sequence number to retransmit.
     */
    private void handleRetransmit(final Integer reliabilitySeqNum) {
        try {
            // Retransmit the packet.
            final DataPacket data = sendBuffer.get(reliabilitySeqNum);
            if (data != null) {
                final DataPacket retransmit = new DataPacket();
                retransmit.copyFrom(data);
                retransmit.setDestinationID(sessionState.getDestinationSessionID());
                endpoint.doSend(retransmit, sessionState);
                statistics.incNumberOfRetransmittedDataPackets();
            }
            else {
                LOG.info("Did not find expected data in sendBuffer [{}]", reliabilitySeqNum);
            }
        }
        catch (Exception e) {
            LOG.warn("Retransmission error", e);
        }
    }

    /**
     * For processing EXP event.
     */
    public void putUnacknowledgedPacketsIntoLossList() {
        synchronized (sendLock) {
            for (final Integer l : sendBuffer.keySet()) {
                senderLossList.insert(l);
            }
        }
    }

    /**
     * The next packet sequence number for data packets. The initial sequence number is {@code 0}.
     */
    private int nextPacketSequenceNumber() {
        return currentSequenceNumber = SeqNum.incrementPacketSeqNum(currentSequenceNumber);
    }

    /**
     * The next reliability sequence number for data packets. The initial sequence number is {@code 0}.
     */
    private int nextReliabilitySequenceNumber() {
        return currentReliabilitySequenceNumber = SeqNum.increment16(currentReliabilitySequenceNumber);
    }

    /**
     * The next order sequence number for data packets. The initial sequence number is {@code 0}.
     */
    private int nextOrderSequenceNumber() {
        return currentOrderSequenceNumber = SeqNum.increment16(currentOrderSequenceNumber);
    }

    public int getCurrentReliabilitySequenceNumber() {
        return currentReliabilitySequenceNumber;
    }

    public int getCurrentSequenceNumber() {
        return currentSequenceNumber;
    }

    public boolean haveAcknowledgementFor(final int reliabilitySequenceNumber) {
        return SeqNum.compare16(reliabilitySequenceNumber, lastAckReliabilitySequenceNumber) < 0;
    }

    public boolean isSentOut(final int packetSeqNum) {
        return SeqNum.comparePacketSeqNum(largestSentSequenceNumber, packetSeqNum) >= 0;
    }

    public boolean haveLostPackets() {
        return !senderLossList.isEmpty();
    }

    /**
     * Wait until the given sequence number has been acknowledged.
     *
     * @param relSequenceNumber the reliability sequence number to wait for.
     * @throws InterruptedException if thread was interrupted while awaiting.
     */
    public void waitForAck(final int relSequenceNumber) throws InterruptedException {
        while (!sessionState.isShutdown() && !haveAcknowledgementFor(relSequenceNumber)) {
            ackLock.lock();
            try {
                ackCondition.await(100, TimeUnit.MICROSECONDS);
            }
            finally {
                ackLock.unlock();
            }
        }
    }

    /**
     * Wait for the next acknowledge.
     *
     * @throws InterruptedException if the thread is interrupted.
     */
    private void waitForAck() throws InterruptedException {
        ackLock.lock();
        try {
            ackCondition.await(200, TimeUnit.MICROSECONDS);
        }
        finally {
            ackLock.unlock();
        }
    }

}
