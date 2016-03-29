package io.lyracommunity.bolt;

import io.lyracommunity.bolt.packet.Ack;
import io.lyracommunity.bolt.packet.Ack2;
import io.lyracommunity.bolt.packet.BoltPacket;
import io.lyracommunity.bolt.packet.ControlPacket;
import io.lyracommunity.bolt.packet.ControlPacketType;
import io.lyracommunity.bolt.packet.DataPacket;
import io.lyracommunity.bolt.packet.KeepAlive;
import io.lyracommunity.bolt.packet.NegativeAcknowledgement;
import io.lyracommunity.bolt.packet.Shutdown;
import io.lyracommunity.bolt.receiver.AckHistoryEntry;
import io.lyracommunity.bolt.receiver.AckHistoryWindow;
import io.lyracommunity.bolt.receiver.PacketHistoryWindow;
import io.lyracommunity.bolt.receiver.PacketPairWindow;
import io.lyracommunity.bolt.receiver.ReceiverLossList;
import io.lyracommunity.bolt.receiver.ReceiverLossListEntry;
import io.lyracommunity.bolt.statistic.BoltStatistics;
import io.lyracommunity.bolt.util.ReceiveBuffer;
import io.lyracommunity.bolt.util.SeqNum;
import io.lyracommunity.bolt.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

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
 * @see BoltSender
 */
public class BoltReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(BoltReceiver.class);
    /**
     * Milliseconds to timeout a new session that stays idle.
     */
    private static final long IDLE_TIMEOUT = 3 * 60 * 1000;

    private final BoltEndPoint endpoint;
    private final BoltSession session;
    private final BoltStatistics statistics;
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

    // EXP event related
    /**
     * Instant when the session was created (for expiry checking)
     */
    private final long sessionUpSince;

    /**
     * Buffer size for storing data.
     */
    private final long bufferSize;

    /**
     * Stores received packets to be sent.
     */
    private final BlockingQueue<BoltPacket> handOffQueue;
    private final Config config;
    /**
     * Microseconds to next EXP event.
     */
    private final long expTimerInterval = 100 * Util.getSYNTime();
    /**
     * Round trip time, calculated from ACK/ACK2 pairs.
     */
    private long roundTripTime = 0;
    /**
     * Round trip time variance.
     */
    private long roundTripTimeVar = roundTripTime / 2;
    /**
     * For storing the arrival time of the last received data packet.
     */
    private volatile long lastDataPacketArrivalTime = 0;
    /**
     * LRSN: A variable to record the largest received data packet sequence
     * number. LRSN is initialized to the initial sequence number minus 1.
     */
    private volatile int largestReceivedRelSeqNumber = 0;
    /**
     * Last Ack number.
     */
    private long lastAckNumber = 0;
    /**
     * largest Ack number ever acknowledged by ACK2
     */
    private volatile long largestAcknowledgedAckNumber = -1;
    /**
     * a variable to record number of continuous EXP time-out events
     */
    private volatile long expCount = 0;
    /**
     * to check the ACK, NAK, or EXP timer
     */
    private long nextACK;
    /**
     * Microseconds to next ACK event.
     */
    private long ackTimerInterval = Util.getSYNTime();
    /**
     * Microseconds to next NAK event.
     */
    private long nakTimerInterval = Util.getSYNTime();
    private long nextNAK;
    private long nextEXP;
    private volatile boolean stopped = false;
    /**
     * (optional) ack interval (see CongestionControl interface)
     */
    private volatile long ackInterval = -1;

    /**
     * Number of received data packets.
     */
    private int n = 0;
    private volatile int ackSequenceNumber = 0;

    /**
     * Create a receiver with a valid {@link BoltSession}
     *
     * @param session
     * @param config
     */
    public BoltReceiver(final BoltSession session, final BoltEndPoint endpoint, final Config config) {
        if (!session.isReady()) throw new IllegalStateException("BoltSession is not ready.");
        this.endpoint = endpoint;
        this.session = session;
        this.sessionUpSince = System.currentTimeMillis();
        this.statistics = session.getStatistics();
        this.ackHistoryWindow = new AckHistoryWindow(16);
        this.packetHistoryWindow = new PacketHistoryWindow(16);
        this.receiverLossList = new ReceiverLossList();
        this.packetPairWindow = new PacketPairWindow(16);
        this.largestReceivedRelSeqNumber = 0;
        this.bufferSize = session.getReceiveBufferSize();
        this.handOffQueue = new ArrayBlockingQueue<>(4 * session.getFlowWindowSize());
        this.config = config;
    }

    /**
     * Starts the sender algorithm
     */
    public Observable<?> start() {
        return Observable.create(subscriber -> {
            try {
                final String s = (session instanceof ServerSession) ? "ServerSession" : "ClientSession";
                Thread.currentThread().setName("Bolt-Receiver-" + s + Util.THREAD_INDEX.incrementAndGet());

                while (session.getSocket() == null) Thread.sleep(100);

                LOG.info("STARTING RECEIVER for " + session);
                nextACK = Util.getCurrentTime() + ackTimerInterval;
                nextNAK = (long) (Util.getCurrentTime() + 1.5 * nakTimerInterval);
                nextEXP = Util.getCurrentTime() + 2 * expTimerInterval;
                ackInterval = session.getCongestionControl().getAckInterval();
                while (!stopped && !subscriber.isUnsubscribed()) {
                    receiverAlgorithm();
                }
            }
            catch (final Exception ex) {
                LOG.error("Receiver exception", ex);
                subscriber.onError(ex);
            }
            LOG.info("STOPPING RECEIVER for " + session);
            subscriber.onCompleted();
            stop();
        });
    }

    /**
     * Packets are written by the endpoint.
     */
    protected void receive(final BoltPacket p) throws IOException {
        final int controlPacketType = p.getControlPacketType();
        if (ControlPacketType.KEEP_ALIVE.getTypeId() == controlPacketType) {
            resetEXPCount();
        }
        else if (ControlPacketType.NAK.getTypeId() == controlPacketType) {
            resetEXPTimer();
        }
        else if (!p.forSender()) {
            // Only allow in here for DataPacket/Ack2/Shutdown
            statistics.beginReceive();
            if (!p.isControlPacket() && LOG.isTraceEnabled()) {
                LOG.trace("++ " + p + " queuesize=" + handOffQueue.size());
            }
            handOffQueue.offer(p);
            statistics.endReceive();
        }
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
    private void receiverAlgorithm() throws InterruptedException, IOException {
        checkTimers();

        // Perform time-bounded UDP receive
        BoltPacket packet = null;
        try {
            packet = handOffQueue.poll(Util.getSYNTime(), TimeUnit.MICROSECONDS);
        }
        catch (InterruptedException e) {
            LOG.info("Polling of hand-off queue was interrupted.");
        }
        if (packet != null) {
            // Reset exp count to 1
            expCount = 1;
            // If there is no unacknowledged data packet, or if this is an ACK or NAK control packet, reset the EXP timer.
            boolean needEXPReset = false;
            if (packet.isControlPacket()) {
                ControlPacket cp = (ControlPacket) packet;
                int cpType = cp.getControlPacketType();
                // TODO is it even possible to reach here. ACK/NACK are received by sender
                if (cpType == ControlPacketType.ACK.getTypeId() || cpType == ControlPacketType.NAK.getTypeId()) {
                    needEXPReset = true;
                }
            }
            if (needEXPReset) {
                nextEXP = Util.getCurrentTime() + expTimerInterval;
            }
            statistics.beginProcess();

            processPacket(packet);

            statistics.endProcess();
        }
    }

    private void checkTimers() throws IOException {
        // Check ACK timer.
        final long currentTime = Util.getCurrentTime();
        if (nextACK < currentTime) {
            nextACK = currentTime + ackTimerInterval;
            processACKEvent(true);
        }
        // Check NAK timer.
        if (nextNAK < currentTime) {
            nextNAK = currentTime + nakTimerInterval;
            processNAKEvent();
        }
        // Check EXP timer.
        if (nextEXP < currentTime) {
            nextEXP = currentTime + expTimerInterval;
            processEXPEvent();
        }
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
    private void processACKEvent(boolean isTriggeredByTimer) throws IOException {
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
            long timeOfLastSentAck = ackHistoryWindow.getTime(lastAckNumber);
            if (Util.getCurrentTime() - timeOfLastSentAck < 2 * roundTripTime) {
                return;
            }
        }
        final long ackSeqNumber;
        // If this ACK is not triggered by ACK timers, send out a light Ack and stop.
        if (!isTriggeredByTimer) {
            ackSeqNumber = sendLightAcknowledgment(ackNumber);
            return;
        }
        else {
            // Pack the packet speed and link capacity into the ACK packet and send it out.
            // 7) Records the ACK number, ackseqNumber and the departure time of this Ack in the ACK History Window.
            ackSeqNumber = sendAcknowledgment(ackNumber);
        }
        AckHistoryEntry sentAckNumber = new AckHistoryEntry(ackSeqNumber, ackNumber, Util.getCurrentTime());
        ackHistoryWindow.add(sentAckNumber);
        // Store ack number for next iteration
        lastAckNumber = ackNumber;
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
     * @throws IOException if shutdown, stop or send of keep-alive fails.
     */
    private void processEXPEvent() throws IOException {
        if (session.getSocket() == null || !session.getSocket().isActive()) return;
        final BoltSender sender = session.getSocket().getSender();
        // Put all the unacknowledged packets in the senders loss list.
        sender.putUnacknowledgedPacketsIntoLossList();
        if (config.isSessionsExpirable() && expCount > 16 && System.currentTimeMillis() - sessionUpSince > IDLE_TIMEOUT) {
            if (!stopped) {
                sendShutdown();
                stop();
                LOG.info("Session {} expired.", session);
                return;
            }
        }
        if (!sender.haveLostPackets()) {
            sendKeepAlive();
        }
        expCount++;
    }

    private void processPacket(final BoltPacket p) throws IOException {
        // 3) Check the packet type and process it according to this.
        if (!p.isControlPacket()) {
            statistics.beginDataProcess();
            onDataPacketReceived((DataPacket) p);
            statistics.endDataProcess();
        }
        else if (p.getControlPacketType() == ControlPacketType.ACK2.getTypeId()) {
            final Ack2 ack2 = (Ack2) p;
            onAck2PacketReceived(ack2);
        }
        else if (p instanceof Shutdown) {
            onShutdown();
        }
    }

    private void onDataPacketReceived(final DataPacket dp) throws IOException {
        n++;

        // TODO remove sout
//        if (dp.isReliable() && dp.getPacketSeqNumber() % 20 == 0) {
//            System.out.println("Received  " + dp.getReliabilitySeqNumber() + " \t\t" + dp.getPacketSeqNumber());
//        }
        if (isArtificialDrop()) {
            LOG.debug("Artificial packet loss, dropping packet");
            return;
        }

        ReceiveBuffer.OfferResult OK = session.getSocket().haveNewData(dp);
        if (!OK.success) {
            LOG.info("Dropping packet [{}  {}] : [{}]", dp.getPacketSeqNumber(), dp.getReliabilitySeqNumber(), OK.message);
            return;
        }

        final long currentDataPacketArrivalTime = Util.getCurrentTime();
        final int currentSeqNumber = dp.getPacketSeqNumber();
        statistics.incNumberOfReceivedDataPackets();

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
            final int relSeqNum = dp.getReliabilitySeqNumber();
            // 6) Number of detected lossed packet
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
            if (ackInterval > 0) {
                if (n % ackInterval == 0) processACKEvent(false);
            }
        }

    }

    /**
     * Write a NAK triggered by a received sequence number that is larger than
     * the largestReceivedRelSeqNumber + 1
     *
     * @param currentRelSequenceNumber the currently received sequence number
     * @throws IOException
     */
    private void sendNAK(final int currentRelSequenceNumber) throws IOException {
        NegativeAcknowledgement nAckPacket = new NegativeAcknowledgement();
        nAckPacket.addLossInfo(SeqNum.increment16(largestReceivedRelSeqNumber), currentRelSequenceNumber);
        nAckPacket.setDestinationID(session.getDestination().getSocketID());
        // Put all the sequence numbers between (but excluding) these two values into the receiver loss list.
        for (int i = SeqNum.increment16(largestReceivedRelSeqNumber);
             SeqNum.compare16(i, currentRelSequenceNumber) < 0;
             i = SeqNum.increment16(i)) {
            final ReceiverLossListEntry detectedLossSeqNumber = new ReceiverLossListEntry(i);
            receiverLossList.insert(detectedLossSeqNumber);
        }
        endpoint.doSend(nAckPacket, session);
        LOG.debug("NAK for {}", currentRelSequenceNumber);
        statistics.incNumberOfNAKSent();
    }

    private void sendNAK(List<Integer> sequenceNumbers) throws IOException {
        if (sequenceNumbers.isEmpty()) return;
        NegativeAcknowledgement nAckPacket = new NegativeAcknowledgement();
        nAckPacket.addLossInfo(sequenceNumbers);
        nAckPacket.setDestinationID(session.getDestination().getSocketID());
        endpoint.doSend(nAckPacket, session);
        statistics.incNumberOfNAKSent();
    }

    private long sendLightAcknowledgment(final int ackNumber) throws IOException {
        Ack acknowledgmentPkt = buildLightAcknowledgement(ackNumber);
        endpoint.doSend(acknowledgmentPkt, session);
        statistics.incNumberOfACKSent();
        return acknowledgmentPkt.getAckSequenceNumber();
    }

    private long sendAcknowledgment(final int ackNumber) throws IOException {
        final Ack ack = Ack.buildAcknowledgement(ackNumber, ++ackSequenceNumber, roundTripTime, roundTripTimeVar,
                bufferSize, session.getDestination().getSocketID(),
                packetPairWindow.getEstimatedLinkCapacity(), packetHistoryWindow.getPacketArrivalSpeed());

        endpoint.doSend(ack, session);

        statistics.incNumberOfACKSent();
        statistics.setPacketArrivalRate(ack.getPacketReceiveRate(), ack.getEstimatedLinkCapacity());
        return ack.getAckSequenceNumber();
    }

    // Builds a "light" Acknowledgement
    private Ack buildLightAcknowledgement(final int ackNumber) {
        return Ack.buildLightAcknowledgement(ackNumber, ++ackSequenceNumber, roundTripTime, roundTripTimeVar, bufferSize,
                session.getDestination().getSocketID());
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
            long ackNumber = entry.getAckNumber();
            largestAcknowledgedAckNumber = Math.max(ackNumber, largestAcknowledgedAckNumber);

            long rtt = entry.getAge();
            if (roundTripTime > 0) roundTripTime = (roundTripTime * 7 + rtt) / 8;
            else roundTripTime = rtt;
            roundTripTimeVar = (roundTripTimeVar * 3 + Math.abs(roundTripTimeVar - rtt)) / 4;
            ackTimerInterval = Math.min(10_000, 4 * roundTripTime + roundTripTimeVar + Util.getSYNTime());
            nakTimerInterval = ackTimerInterval;
            statistics.setRTT(roundTripTime, roundTripTimeVar);
        }
    }

    private void sendKeepAlive() throws IOException {
        final KeepAlive ka = new KeepAlive();
        ka.setDestinationID(session.getDestination().getSocketID());
        endpoint.doSend(ka, session);
    }

    private void sendShutdown() throws IOException {
        final Shutdown s = new Shutdown();
        s.setDestinationID(session.getDestination().getSocketID());
        endpoint.doSend(s, session);
    }

    protected void resetEXPTimer() {
        nextEXP = Util.getCurrentTime() + expTimerInterval;
        expCount = 0;
    }

    protected void resetEXPCount() {
        expCount = 0;
    }

    public void setAckInterval(long ackInterval) {
        this.ackInterval = ackInterval;
    }

    private void onShutdown() throws IOException {
        stop();
    }

    public void stop() {
        stopped = true;
        try {
            session.getSocket().close();
        }
        catch (IOException ex) {
            LOG.warn("Could not shutdown cleanly [{}]", ex.getMessage());
        }
        // Stop our sender as well.
        session.getSocket().getSender().stop();
    }

    private boolean isArtificialDrop() {
        final float dropRate = config.getPacketDropRate();
        return dropRate > 0 && (n % dropRate < 1f);
    }

    public String toString() {
        return "BoltReceiver " + session + "\n" +
                "LossList: " + receiverLossList;
    }

}
