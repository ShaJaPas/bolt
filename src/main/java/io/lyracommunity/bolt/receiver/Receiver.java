package io.lyracommunity.bolt.receiver;

import io.lyracommunity.bolt.ChannelOut;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.packet.*;
import io.lyracommunity.bolt.sender.Sender;
import io.lyracommunity.bolt.session.Session;
import io.lyracommunity.bolt.session.SessionState;
import io.lyracommunity.bolt.statistic.BoltStatistics;
import io.lyracommunity.bolt.util.ReceiveBuffer;
import io.lyracommunity.bolt.util.SeqNum;
import io.lyracommunity.bolt.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * Receiver half of a Bolt entity.
 * <p>
 * The receiver receives both data packets and control packets, and sends
 * out control packets according to the received packets and the timers.
 * <p>
 * The receiver is also responsible for triggering and processing all control
 * events, including congestion control and reliability control, and
 * their related mechanisms such as RTT estimation, bandwidth estimation,
 * acknowledging and retransmission.
 *
 * @see Sender
 */
public class Receiver {

    private static final Logger LOG = LoggerFactory.getLogger(Receiver.class);


    private final ChannelOut     endpoint;
    private final SessionState   sessionState;
    private final BoltStatistics statistics;
    private final Sender         sender;

    /**
     * Record seqNo of detected lost data and latest feedback time.
     */
    private final ReceiverLossList receiverLossList;

    /**
     * Record each sent ACK and the sent time.
     */
    private final AckHistoryWindow ackHistoryWindow;

    // ACK event related
    /**
     * Packet history window that stores the time interval between the current and the last seq.
     */
    private final PacketHistoryWindow packetHistoryWindow;

    /**
     * Records the time interval between each probing pair compute the
     * median packet pair interval of the last 16 packet pair intervals (PI)
     * and the estimate link capacity.(packet/s)
     */
    private final PacketPairWindow packetPairWindow;

    /**
     * Buffer size for storing data.
     */
    private final long bufferSize;

    /**
     * Stores received packets to be sent.
     */
    private final BlockingQueue<BoltPacket> handOffQueue;
    private final Config                    config;
    private final ReceiveBuffer             receiveBuffer;
    private final EventTimers               timers;
    /**
     * Round trip time, calculated from ACK/ACK2 pairs.
     */
    private          long roundTripTime                = 0;
    /**
     * Round trip time variance.
     */
    private          long roundTripTimeVar             = roundTripTime / 2;
    /**
     * For storing the arrival time of the last received data packet.
     */
    private volatile long lastDataPacketArrivalTime    = 0;
    /**
     * LRSN: The largest received reliability sequence number.
     */
    private volatile int  largestReceivedRelSeqNumber  = 0;
    /**
     * Last Ack number.
     */
    private          long lastAckNumber                = 0;
    /**
     * largest Ack number ever acknowledged by ACK2
     */
    private volatile long largestAcknowledgedAckNumber = -1;
    /**
     * Number of reliable received data packets.
     */
    private          int  reliableN                    = 0;
    private volatile int  ackSequenceNumber            = 0;

    /**
     * Create a receiver for a particular {@link Session}.
     *
     * @param config       bolt configuration.
     * @param sessionState the owning session state.
     * @param endpoint     the network endpoint.
     * @param sender       the matching sender.
     * @param statistics   statistics for the session.
     * @param timers       the ACK/NAK/EXP event timers.
     */
    public Receiver(final Config config, final SessionState sessionState, final ChannelOut endpoint,
                    final Sender sender, final BoltStatistics statistics, final EventTimers timers) {
        this.endpoint = endpoint;
        this.sessionState = sessionState;
        this.sender = sender;
        this.config = config;
        this.statistics = statistics;
        this.timers = timers;
        this.ackHistoryWindow = new AckHistoryWindow(16);
        this.packetHistoryWindow = new PacketHistoryWindow(16);
        this.receiverLossList = new ReceiverLossList();
        this.packetPairWindow = new PacketPairWindow(16);
        this.largestReceivedRelSeqNumber = 0;
        this.bufferSize = sessionState.getReceiveBufferSize();
        this.handOffQueue = new ArrayBlockingQueue<>(4 * sessionState.getFlowWindowSize());
        this.receiveBuffer = new ReceiveBuffer(2 * sessionState.getFlowWindowSize());
    }

    public DataPacket pollReceiveBuffer(final int timeout, final TimeUnit unit) throws InterruptedException {
        return receiveBuffer.poll(timeout, unit);
    }

    public DataPacket pollReceiveBuffer() {
        return receiveBuffer.poll();
    }

    /**
     * Packets are written by the endpoint.
     *
     * @param received the packet to receive.
     */
    public void receive(final BoltPacket received) {
        statistics.beginReceive();
        if (!received.isControlPacket() && LOG.isTraceEnabled()) {
            LOG.trace("++ {}  QueueSize={}", received, handOffQueue.size());
        }
        handOffQueue.offer(received);
        statistics.endReceive();
    }

    /**
     * Data Receiving Algorithm:
     * <ol>
     * <li> Query the system time to check if ACK, NAK, or EXP timer has
     * expired. If there is any, process the event (as described below
     * in this section) and reset the associated time variables. For
     * ACK, also check the ACK packet interval.
     * <li> Start time bounded UDP receiving. If no packet arrives, go to 1).
     * 1) Reset the ExpCount to 1. If there is no unacknowledged data
     * packet, or if this is an ACK or NAK control packet, reset the EXP
     * timer.
     * <li> Check the flag bit of the packet header. If it is a control
     * packet, process it according to its type and go to 1).
     * <li> If the sequence number of the current data packet is 16n + 1,
     * where n is an integer, record the time interval between this
     * packet and the last data packet in the Packet Pair Window.
     * <li> Record the packet arrival time in PKT History Window.
     * <li>
     * a. If the sequence number of the current data packet is greater
     * than LRSN + 1, put all the sequence numbers between (but
     * excluding) these two values into the receiver's loss list and
     * send them to the sender in an NAK packet. <br/>
     * b. If the sequence number is less than LRSN, remove it from the
     * receiver's loss list.
     * <li> Update LRSN. Go to 1).
     * </ol>
     */
    boolean receiverAlgorithm(final boolean awaitPoll) throws InterruptedException, ExpiryException, IOException {
        boolean addedData = false;
        if (sessionState.isActive()) {
            // Query for timer events.
            checkTimers();

            // Perform time-bounded UDP receive.
            final BoltPacket packet = awaitPoll
                    ? handOffQueue.poll(Util.getSYNTime(), TimeUnit.MICROSECONDS)
                    : handOffQueue.poll();

            if (packet != null) {
                // Reset EXP count for any packet.
                timers.resetEXPCount();

                statistics.beginProcess();
                addedData = processPacket(packet);
                statistics.endProcess();
            }
        }
        return addedData;
    }

    private void checkTimers() throws IOException, ExpiryException {
        timers.ensureInit();

        final long currentTime = Util.currentTimeMicros();
        // Check ACK timer.
        if (timers.checkIsNextAck(currentTime)) processACKEvent(true);
        // Check NAK timer.
        if (timers.checkIsNextNak(currentTime)) processNAKEvent();
        // Check EXP timer.
        if (timers.checkIsNextExp(currentTime)) processEXPEvent();
    }

    /**
     * ACK Event Processing:
     * <ol>
     * <li> Find the sequence number prior to which all the packets have been
     * received by the receiver (ACK number) according to the following
     * rule: if the receiver's loss list is empty, the ACK number is LRSN
     * + 1; otherwise it is the smallest sequence number in the
     * receiver's loss list.
     * <li> If (a) the ACK number equals to the largest ACK number ever
     * acknowledged by ACK2, or (b) it is equal to the ACK number in the
     * last ACK and the time interval between this two ACK packets is
     * less than 2 RTTs, stop (do not send this ACK).
     * <li> Assign this ACK a unique increasing ACK sequence number. Pack the
     * ACK packet with RTT, RTT Variance, and flow window size (available
     * receiver buffer size). If this ACK is not triggered by ACK timers,
     * send out this ACK and stop.
     * <li> Calculate the packet arrival speed according to the following
     * algorithm:
     * Calculate the median value of the last 16 packet arrival
     * intervals (AI) using the values stored in PKT History Window.
     * In these 16 values, remove those either greater than AI*8 or
     * less than AI/8. If more than 8 values are left, calculate the
     * average of the left values AI', and the packet arrival speed is
     * 1/AI' (number of packets per second). Otherwise, return 0.
     * <li> Calculate the estimated link capacity according to the following
     * algorithm:
     * Calculate the median value of the last 16 packet pair
     * intervals (PI) using the values in Packet Pair Window, and the
     * link capacity is 1/PI (number of packets per second).
     * <li> Pack the packet arrival speed and estimated link capacity into the
     * ACK packet and send it out.
     * <li> Record the ACK sequence number, ACK number and the departure time
     * of this ACK in the ACK History Window.
     * </ol>
     */
    private void processACKEvent(final boolean isTriggeredByTimer) throws IOException {
        // 1) Find the sequence number prior to which all the packets have been received
        final ReceiverLossListEntry entry = receiverLossList.getFirstEntry();

        final int ackNumber = (entry == null)
                ? SeqNum.increment16(largestReceivedRelSeqNumber)
                : entry.getSequenceNumber();

        // 2a) If ackNumber equals to the largest sequence number ever acknowledged by ACK2
        if (ackNumber == largestAcknowledgedAckNumber) {
            // Do not send this ACK
            return;
        }
        else if (ackNumber == lastAckNumber) {
            // Or it is equals to the ackNumber in the last ACK and the time interval
            // between these two ACK packets is less than 2 RTTs, do not send(stop).
            final long timeOfLastSentAck = ackHistoryWindow.getTime(lastAckNumber);

            if (Util.currentTimeMicros() - timeOfLastSentAck < 2 * roundTripTime) {
                return;
            }
        }

        // If this ACK is not triggered by ACK timers, send out a light Ack and stop.
        if (!isTriggeredByTimer) {
            sendLightAcknowledgment(ackNumber);
        }
        else {
            // Pack the packet speed and link capacity into the ACK packet and send it out.
            // 7) Records the ACK number, ackseqNumber and the departure time of this Ack in the ACK History Window.
            final long ackSeqNumber = sendAcknowledgment(ackNumber);

            AckHistoryEntry sentAckNumber = new AckHistoryEntry(ackSeqNumber, ackNumber, Util.currentTimeMicros());
            ackHistoryWindow.add(sentAckNumber);
            // Store ack number for next iteration
            lastAckNumber = ackNumber;
        }
    }

    /**
     * NAK Event Processing:
     * <p>
     * Search the receiver's loss list, find out all those sequence numbers
     * whose last feedback time is k*RTT before, where k is initialized as 2
     * and increased by 1 each time the number is fed back. Compress
     * (according to section 6.4) and send these numbers back to the sender
     * in an NAK packet.
     */
    private void processNAKEvent() throws IOException {
        final List<Integer> seqNumbers = receiverLossList.getFilteredSequenceNumbers(roundTripTime, true);
        sendNAK(seqNumbers);
    }

    /**
     * EXP Event Processing:
     * <ol>
     * <li> Put all the unacknowledged packets into the sender's loss list.
     * <li> If (ExpCount > 16) and at least 3 seconds has elapsed since that
     * last time when ExpCount is reset to 1, or, 3 minutes has elapsed,
     * close the Bolt connection and exit.
     * <li> If the sender's loss list is empty, send a keep-alive packet to
     * the peer side.
     * <li> Increase ExpCount by 1.
     * </ol>
     *
     * @throws IOException     if shutdown, stop or send of keep-alive fails.
     * @throws ExpiryException if the session has just expired.
     */
    private void processEXPEvent() throws IOException, ExpiryException {
        if (!sessionState.isActive()) return;
        // Put all the unacknowledged packets in the senders loss list.
        sender.putUnacknowledgedPacketsIntoLossList();
        if (timers.isSessionExpired()) {
            LOG.warn("Session {} expired.", sessionState);
            throw new ExpiryException("Session expired.");
        }
        else if (!sender.haveLostPackets()) {
            sendKeepAlive();
        }
        timers.incrementExpCount();
    }

    private boolean processPacket(final BoltPacket p) throws IOException {
        boolean addedData = false;
        // 3) Check the packet type and process it according to this.
        if (!p.isControlPacket()) {
            statistics.beginDataProcess();
            addedData = onDataPacketReceived((DataPacket) p);
            statistics.endDataProcess();
        }
        else {
            final PacketType packetType = p.getPacketType();
            // If this is an ACK or NAK control packet, reset the EXP timer.
            if (PacketType.NAK == packetType || PacketType.ACK == packetType) {
                timers.resetEXPTimer();
            }
            else if (p.getPacketType() == PacketType.ACK2) {
                final Ack2 ack2 = (Ack2) p;
                onAck2PacketReceived(ack2);
            }
        }
        return addedData;
    }

    /**
     * @return true if new data was submitted to the receive buffer, otherwise false.
     */
    private boolean onDataPacketReceived(final DataPacket dp) throws IOException {

        final ReceiveBuffer.OfferResult OK = receiveBuffer.offer(dp);
        if (!OK.success) {
            if (OK == ReceiveBuffer.OfferResult.ERROR_DUPLICATE) statistics.incNumberOfDuplicateDataPackets();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Dropping packet [{}  {}] : [{}]", dp.getPacketSeqNumber(), dp.getReliabilitySeqNumber(), OK.message);
            }
            return false;
        }

        final long currentDataPacketArrivalTime = Util.currentTimeMicros();
        final int currentSeqNumber = dp.getPacketSeqNumber();
        statistics.addReceived(dp.getClassID(), dp.getDataLength());

        // 4) If the seqNo of the current data packet is 16n+1, record the time interval
        // between this packet and the last data packet in the packet pair window.
        if ((currentSeqNumber % 16) == 1 && lastDataPacketArrivalTime > 0) {
            final long interval = currentDataPacketArrivalTime - lastDataPacketArrivalTime;
            packetPairWindow.add(interval);
        }

        // 5) Record the packet arrival time in the PKT History Window.
        packetHistoryWindow.add(currentDataPacketArrivalTime);
        // Store current time
        lastDataPacketArrivalTime = currentDataPacketArrivalTime;

        if (dp.isReliable()) {
            reliableN++;
            final int relSeqNum = dp.getReliabilitySeqNumber();
            // 6) Number of detected lost packet
            if (SeqNum.compare16(relSeqNum, SeqNum.increment16(largestReceivedRelSeqNumber)) > 0) {
                // 6.a) If the number of the current data packet is greater than LSRN + 1,
                // put all the sequence numbers between (but excluding) these two values
                // into the receiver's loss list and send them to the sender in an NAK packet
                sendNAK(relSeqNum);
            }
            else if (SeqNum.compare16(relSeqNum, largestReceivedRelSeqNumber) < 0) {
                // 6.b) If the sequence number is less than LRSN, remove it from the receiver's loss list.
                receiverLossList.remove(relSeqNum);
            }

            // 7) Update the LRSN
            if (SeqNum.compare16(relSeqNum, largestReceivedRelSeqNumber) > 0) {
                largestReceivedRelSeqNumber = relSeqNum;
            }

            // 8) Need to send an ACK? Some cc algorithms use this.
            if (config.getAckInterval() > 0) {
                if (reliableN % config.getAckInterval() == 0) processACKEvent(false);
            }
        }
        return true;
    }

    /**
     * Write a NAK triggered by a received sequence number that is larger than
     * the largestReceivedRelSeqNumber + 1
     *
     * @param currentRelSequenceNumber the currently received sequence number
     * @throws IOException
     */
    private void sendNAK(final int currentRelSequenceNumber) throws IOException {
        final Nak nAckPacket = new Nak();
        nAckPacket.addLossRange(SeqNum.increment16(largestReceivedRelSeqNumber), currentRelSequenceNumber);
        nAckPacket.setDestinationID(sessionState.getDestinationSessionID());
        // Put all the sequence numbers between (but excluding) these two values into the receiver loss list.
        for (int i = SeqNum.increment16(largestReceivedRelSeqNumber);
             SeqNum.compare16(i, currentRelSequenceNumber) < 0;
             i = SeqNum.increment16(i)) {
            final ReceiverLossListEntry detectedLossSeqNumber = new ReceiverLossListEntry(i);
            receiverLossList.insert(detectedLossSeqNumber);
        }
        endpoint.doSend(nAckPacket, sessionState);
        LOG.debug("NAK for {}", currentRelSequenceNumber);
        statistics.incNumberOfNAKSent();
    }

    private void sendNAK(final List<Integer> reliabilitySeqNumbers) throws IOException {
        if (reliabilitySeqNumbers.isEmpty()) return;
        final List<Integer> toSend = (reliabilitySeqNumbers.size() > 300)
                ? reliabilitySeqNumbers.subList(0, 300) : reliabilitySeqNumbers;
        final Nak nAckPacket = new Nak();
        nAckPacket.addLossList(toSend);
        nAckPacket.setDestinationID(sessionState.getDestinationSessionID());
        endpoint.doSend(nAckPacket, sessionState);
        statistics.incNumberOfNAKSent();
    }

    private long sendLightAcknowledgment(final int ackNumber) throws IOException {
        final Ack acknowledgmentPkt = buildLightAcknowledgement(ackNumber);
        endpoint.doSend(acknowledgmentPkt, sessionState);
        statistics.incNumberOfACKSent();
        return acknowledgmentPkt.getAckSequenceNumber();
    }

    private long sendAcknowledgment(final int ackNumber) throws IOException {
        final Ack ack = Ack.buildAcknowledgement(ackNumber, ++ackSequenceNumber, roundTripTime, roundTripTimeVar,
                bufferSize, sessionState.getDestinationSessionID(),
                packetPairWindow.getEstimatedLinkCapacity(), packetHistoryWindow.getPacketArrivalSpeed());

        endpoint.doSend(ack, sessionState);

        statistics.incNumberOfACKSent();
        statistics.setPacketArrivalRate(ack.getPacketReceiveRate(), ack.getEstimatedLinkCapacity());
        return ack.getAckSequenceNumber();
    }

    // Builds a "light" Acknowledgement
    private Ack buildLightAcknowledgement(final int ackNumber) {
        return Ack.buildLightAcknowledgement(ackNumber, ++ackSequenceNumber, roundTripTime, roundTripTimeVar, bufferSize,
                sessionState.getDestinationSessionID());
    }

    /**
     * On ACK2 packet received:
     * <ol>
     * <li> Locate the related ACK in the ACK History Window according to the
     * ACK sequence number in this ACK2.
     * <li> Update the largest ACK number ever been acknowledged.
     * <li> Calculate new rtt according to the ACK2 arrival time and the ACK
     * departure time, and update the RTT value as: RTT = (RTT * 7 + rtt) / 8.
     * <li> Update RTTVar by: RTTVar = (RTTVar * 3 + abs(RTT - rtt)) / 4.
     * <li> Update both ACK and NAK period to 4 * RTT + RTTVar + SYN.
     * </ol>
     */
    private void onAck2PacketReceived(Ack2 ack2) {
        final AckHistoryEntry entry = ackHistoryWindow.getEntry(ack2.getAckSequenceNumber());
        if (entry != null) {
            final long ackNumber = entry.getAckNumber();
            largestAcknowledgedAckNumber = Math.max(ackNumber, largestAcknowledgedAckNumber);

            final long rtt = entry.getAge();
            roundTripTime = (roundTripTime > 0) ? ((roundTripTime * 7 + rtt) / 8) : rtt;
            roundTripTimeVar = (roundTripTimeVar * 3 + Math.abs(roundTripTimeVar - rtt)) / 4;

            // Calculate ack timer interval and update timer with this.
            timers.updateTimerIntervals(roundTripTime, roundTripTimeVar);
            statistics.setRTT(roundTripTime, roundTripTimeVar);
        }
    }

    private void sendKeepAlive() throws IOException {
        final KeepAlive ka = new KeepAlive();
        ka.setDestinationID(sessionState.getDestinationSessionID());
        endpoint.doSend(ka, sessionState);
    }

    public String toString() {
        return "Receiver " + sessionState + "\n" + "LossList: " + receiverLossList;
    }

}
