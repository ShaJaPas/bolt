package io.lyracommunity.bolt.sender;

import io.lyracommunity.bolt.BoltClient;
import io.lyracommunity.bolt.BoltEndPoint;
import io.lyracommunity.bolt.CongestionControl;
import io.lyracommunity.bolt.packet.Ack;
import io.lyracommunity.bolt.packet.Ack2;
import io.lyracommunity.bolt.packet.BoltPacket;
import io.lyracommunity.bolt.packet.DataPacket;
import io.lyracommunity.bolt.packet.NegAck;
import io.lyracommunity.bolt.packet.PacketType;
import io.lyracommunity.bolt.receiver.BoltReceiver;
import io.lyracommunity.bolt.session.SessionState;
import io.lyracommunity.bolt.statistic.BoltStatistics;
import io.lyracommunity.bolt.util.SeqNum;
import io.lyracommunity.bolt.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;


/**
 * Sender half of a Bolt entity.
 * <p>
 * The sender sends (and retransmits) application data according to the
 * flow control and congestion control.
 *
 * @see BoltReceiver
 */
public class BoltSender {

    private static final Logger LOG = LoggerFactory.getLogger(BoltClient.class);

    private final BoltEndPoint endpoint;

//    private final BoltSession session;

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
    private final ReentrantLock ackLock = new ReentrantLock();
    private final Condition ackCondition = ackLock.newCondition();

    /**
     * For generating data packet sequence numbers.
     */
    private volatile int currentSequenceNumber = 0;

    /**
     * For generating reliability sequence numbers.
     */
    private volatile int currentReliabilitySequenceNumber = 0;

    /**
     * For generating order sequence numbers.
     */
    private volatile int currentOrderSequenceNumber = 0;

    /**
     * The largest data packet sequence number that has actually been sent out.
     */
    private volatile int largestSentSequenceNumber = -1;

    /**
     * Last acknowledge number, initialised to the initial sequence number.
     */
    private volatile int lastAckReliabilitySequenceNumber;
    private volatile boolean started = false;

    /**
     * Used to signal that the sender should start to send
     */
    private volatile CountDownLatch startLatch = new CountDownLatch(1);

    private final CongestionControl cc;

    private final SessionState sessionState;


    public BoltSender(final SessionState state, final BoltEndPoint endpoint, final CongestionControl cc) {
        this.endpoint = endpoint;
        this.cc = cc;
        this.statistics = state.getStatistics();
        this.sessionState = state;
        this.senderLossList = new SenderLossList();
        this.sendBuffer = new ConcurrentHashMap<>(sessionState.getFlowWindowSize(), 0.75f, 2);

        this.lastAckReliabilitySequenceNumber = 0;
        this.currentSequenceNumber = state.getInitialSequenceNumber() - 1;

        final int chunkSize = endpoint.getConfig().getDatagramSize() - 24;
        this.flowWindow = new FlowWindow(sessionState.getFlowWindowSize(), chunkSize);
    }

    /**
     * Start the sender thread.
     */
    public void start() {
        LOG.info("STARTING SENDER for {}", sessionState);
        startLatch.countDown();
        started = true;
    }

    /**
     * Starts the sender algorithm.
     */
    public Observable<?> doStart(final String threadSuffix) {
        if (!sessionState.isReady()) throw new IllegalStateException("BoltSession is not ready.");
        return Observable.create(subscriber -> {
            try {
                Thread.currentThread().setName("Bolt-Sender-" + threadSuffix);

                final Supplier<Boolean> stopped = subscriber::isUnsubscribed;
//                while (!stopped.get()) {
                // Wait until explicitly (re)started.
                startLatch.await();
                senderAlgorithm(stopped);
//                }
            }
            catch (InterruptedException ex) {
                LOG.info("Finished with an interrupt {}", (Object)ex);
            }
            catch (IOException ex) {
                LOG.error("Sender error", ex);
                subscriber.onError(ex);
            }
            LOG.info("STOPPING SENDER for {}", threadSuffix);
            subscriber.onCompleted();
        });
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
     * @param timeout
     * @param units
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public void sendPacket(final DataPacket src, int timeout, TimeUnit units) throws IOException, InterruptedException {
        if (!started) start();
        src.setDestinationID(sessionState.getDestinationSocketID());
        src.setPacketSeqNumber(nextPacketSequenceNumber());
        src.setReliabilitySeqNumber(src.isReliable() ? nextReliabilitySequenceNumber() : 0);
        src.setOrderSeqNumber(src.isOrdered() ? nextOrderSequenceNumber() : 0);

        boolean complete = false;
        while (!complete) {
            complete = flowWindow.tryProduce(src);
            if (!complete) Thread.sleep(1);
        }
    }

    /**
     * Receive a packet from server from the peer.
     */
    public void receive(final BoltPacket p) throws IOException {
        if (p.isControlPacket()) {
            if (PacketType.ACK == p.getPacketType()) {
                onAcknowledge((Ack) p);
            }
            else if (PacketType.NAK == p.getPacketType()) {
                onNAKPacketReceived((NegAck) p);
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
    private void onAcknowledge(final Ack ack) throws IOException {
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
    private void onNAKPacketReceived(NegAck nak) {
        for (Integer i : nak.getDecodedLossInfo()) {
            senderLossList.insert(i);
        }
        cc.onLoss(nak.getDecodedLossInfo(), getCurrentReliabilitySequenceNumber());
        statistics.incNumberOfNAKReceived();

        if (LOG.isDebugEnabled()) {
            LOG.debug("NAK for {} packets lost, set send period to {}",
                    nak.getDecodedLossInfo().size(), cc.getSendInterval());
        }
    }

    private void sendAck2(long ackSequenceNumber) throws IOException {
        final Ack2 ackOfAckPkt = Ack2.build(ackSequenceNumber, sessionState.getDestinationSocketID());
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
     * @throws IOException          on failure to send the DataPacket.
     */
    private void senderAlgorithm(final Supplier<Boolean> stopped) throws IOException {
        while (!stopped.get()) {
            try {
                final long iterationStart = Util.getCurrentTime();
                // If the sender's loss list is not empty
                final Integer entry = senderLossList.getFirstEntry();
                if (entry != null) {
                    handleRetransmit(entry);
                }
                else {
                    // If the number of unacknowledged data packets does not exceed the congestion
                    // and the flow window sizes, pack a new packet.
                    int unAcknowledged = unacknowledged.get();

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
                        waitForAck();
                    }
                }

                // Wait
                if (largestSentSequenceNumber % 16 != 0) {
                    final long snd = (long) cc.getSendInterval();
                    long passed = Util.getCurrentTime() - iterationStart;
                    int x = 0;
                    while (snd - passed > 0) {
                        // Can't wait with microsecond precision :(
                        if (x == 0) {
                            statistics.incNumberOfCCSlowDownEvents();
                            x++;
                        }
                        passed = Util.getCurrentTime() - iterationStart;
                        if (stopped.get()) return;
                    }
                }
            }
            catch (InterruptedException e) {
                LOG.info("Sender caught an interrupt {}", e.getMessage());
            }
        }
    }

    /**
     * Re-transmit an entry from the sender loss list.
     *
     * @param relSeqNumber reliability sequence number to retransmit.
     */
    protected void handleRetransmit(final Integer relSeqNumber) {
        try {
            // Retransmit the packet and remove it from the list.
            final DataPacket data = sendBuffer.get(relSeqNumber);
            if (data != null) {
                final DataPacket retransmit = new DataPacket();
                retransmit.copyFrom(data);
                retransmit.setDestinationID(sessionState.getDestinationSocketID());
                endpoint.doSend(retransmit, sessionState);
                statistics.incNumberOfRetransmittedDataPackets();
            }
            else {
                LOG.warn("Did not find expected data in sendBuffer [{}]", relSeqNumber);
            }
        }
        catch (Exception e) {
            LOG.warn("Retransmission error", e);
        }
    }

    /**
     * For processing EXP event (see spec. p 13).
     */
    public void putUnacknowledgedPacketsIntoLossList() {
        synchronized (sendLock) {
            for (final Integer l : sendBuffer.keySet()) {
                senderLossList.insert(l);
            }
        }
    }

    /**
     * The next sequence number for data packets.
     * The initial sequence number is "0".
     */
    public int nextPacketSequenceNumber() {
        return currentSequenceNumber = SeqNum.incrementPacketSeqNum(currentSequenceNumber);
    }

    /**
     * The next sequence number for data packets.
     * The initial sequence number is "0".
     */
    public int nextReliabilitySequenceNumber() {
        return currentReliabilitySequenceNumber = SeqNum.increment16(currentReliabilitySequenceNumber);
    }

    /**
     * The next sequence number for data packets.
     * The initial sequence number is "0".
     */
    public int nextOrderSequenceNumber() {
        return currentOrderSequenceNumber = SeqNum.increment16(currentOrderSequenceNumber);
    }

    public int getCurrentReliabilitySequenceNumber() {
        return currentReliabilitySequenceNumber;
    }

    public int getCurrentSequenceNumber() {
        return currentSequenceNumber;
    }

    public boolean haveAcknowledgementFor(int reliabilitySequenceNumber) {
        return SeqNum.compare16(reliabilitySequenceNumber, lastAckReliabilitySequenceNumber) <= 0;
    }

    public boolean isSentOut(int sequenceNumber) {
        return SeqNum.comparePacketSeqNum(largestSentSequenceNumber, sequenceNumber) >= 0;
    }

    public boolean haveLostPackets() {
        return !senderLossList.isEmpty();
    }

    /**
     * Wait until the given sequence number has been acknowledged.
     *
     * @throws InterruptedException
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
