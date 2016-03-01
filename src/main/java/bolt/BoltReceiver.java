package bolt;

import bolt.packets.*;
import bolt.packets.ControlPacket.ControlPacketType;
import bolt.packets.Shutdown;
import bolt.receiver.*;
import bolt.statistic.BoltStatistics;
import bolt.statistic.MeanValue;
import bolt.util.BoltThreadFactory;
import bolt.util.SequenceNumber;
import bolt.util.Util;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    private static final Logger LOG = Logger.getLogger(BoltReceiver.class.getName());

    /**
     * If set to true connections will not expire, but will only be
     * closed by a Shutdown message
     */
    public static boolean connectionExpiryDisabled = false;
    private final BoltEndPoint endpoint;
    private final BoltSession session;
    private final BoltStatistics statistics;

    //every nth packet will be discarded... for testing only of course
    public static int dropRate = 0;

    /** record seqNo of detected lost data and latest feedback time */
    private final ReceiverLossList receiverLossList;

    /** record each sent ACK and the sent time */
    private final AckHistoryWindow ackHistoryWindow;

    /** Packet history window that stores the time interval between the current and the last seq. */
    private final PacketHistoryWindow packetHistoryWindow;

    //ACK event related
    /**
     * Records the time interval between each probing pair compute the
     * median packet pair interval of the last 16 packet pair intervals (PI)
     * and the estimate link capacity.(packet/s)
     */
    private final PacketPairWindow packetPairWindow;

    //instant when the session was created (for expiry checking)
    private final long sessionUpSince;

    //EXP event related
    /** Milliseconds to timeout a new session that stays idle */
    private static final long IDLE_TIMEOUT = 3 * 60 * 1000;

    /** buffer size for storing data */
    private final long bufferSize;

    /** stores received packets to be sent */
    private final BlockingQueue<BoltPacket> handOffQueue;

    /** estimated link capacity */
    long estimateLinkCapacity;
    /** the packet arrival rate */
    long packetArrivalSpeed;
    /** round trip time, calculated from ACK/ACK2 pairs */
    long roundTripTime = 0;
    /** round trip time variance */
    long roundTripTimeVar = roundTripTime / 2;
    /** for storing the arrival time of the last received data packet */
    private volatile long lastDataPacketArrivalTime = 0;

    /**
     * LRSN: A variable to record the largest received data packet sequence
     * number. LRSN is initialized to the initial sequence number minus 1.
     */
    private volatile int largestReceivedSeqNumber = 0;

    /**
     * last Ack number
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
     * microseconds to next ACK event
     */
    private long ackTimerInterval = Util.getSYNTime();

    /**
     * microseconds to next NAK event
     */
    private long nakTimerInterval = Util.getSYNTime();
    private long nextNAK;

    /**
     * microseconds to next EXP event
     */
    private long expTimerInterval = 100 * Util.getSYNTime();
    private long nextEXP;

    private volatile boolean stopped = false;

    /**
     * (optional) ack interval (see CongestionControl interface)
     */
    private volatile long ackInterval = -1;

    private final boolean storeStatistics;
    private MeanValue dgReceiveInterval;
    private MeanValue dataPacketInterval;
    private MeanValue processTime;
    private MeanValue dataProcessTime;

    /**
     * Number of received data packets.
     */
    private int n = 0;
    private volatile int ackSequenceNumber = 0;

    /**
     * create a receiver with a valid {@link BoltSession}
     *
     * @param session
     */
    public BoltReceiver(final BoltSession session, final BoltEndPoint endpoint) {
        if (!session.isReady()) throw new IllegalStateException("BoltSession is not ready.");
        this.endpoint = endpoint;
        this.session = session;
        this.sessionUpSince = System.currentTimeMillis();
        this.statistics = session.getStatistics();
        this.ackHistoryWindow = new AckHistoryWindow(16);
        this.packetHistoryWindow = new PacketHistoryWindow(16);
        this.receiverLossList = new ReceiverLossList();
        this.packetPairWindow = new PacketPairWindow(16);
        this.largestReceivedSeqNumber = session.getInitialSequenceNumber() - 1;
        this.bufferSize = session.getReceiveBufferSize();
        this.handOffQueue = new ArrayBlockingQueue<>(4 * session.getFlowWindowSize());
        this.storeStatistics = Boolean.getBoolean("bolt.receiver.storeStatistics");
        initMetrics();
        start();
    }

    private void initMetrics() {
        if (!storeStatistics) return;
        dgReceiveInterval = new MeanValue("RECEIVER: Bolt receive interval");
        statistics.addMetric(dgReceiveInterval);
        dataPacketInterval = new MeanValue("RECEIVER: Data packet interval");
        statistics.addMetric(dataPacketInterval);
        processTime = new MeanValue("RECEIVER: Bolt packet process time");
        statistics.addMetric(processTime);
        dataProcessTime = new MeanValue("RECEIVER: Data packet process time");
        statistics.addMetric(dataProcessTime);
    }

    /** Starts the sender algorithm */
    private void start() {
        final Runnable r = () -> {
            try {
                while (session.getSocket() == null) Thread.sleep(100);

                LOG.info("STARTING RECEIVER for " + session);
                nextACK = Util.getCurrentTime() + ackTimerInterval;
                nextNAK = (long) (Util.getCurrentTime() + 1.5 * nakTimerInterval);
                nextEXP = Util.getCurrentTime() + 2 * expTimerInterval;
                ackInterval = session.getCongestionControl().getAckInterval();
                while (!stopped) {
                    receiverAlgorithm();
                }
            } catch (Exception ex) {
                LOG.log(Level.SEVERE, "", ex);
            }
            LOG.info("STOPPING RECEIVER for " + session);
        };
        String s = (session instanceof ServerSession) ? "ServerSession" : "ClientSession";
        final Thread receiverThread = BoltThreadFactory.get().newThread(r, "Receiver-" + s, false);
        receiverThread.start();
    }

    /**
     * Packets are written by the endpoint.
     */
    protected void receive(BoltPacket p) throws IOException {
        if (storeStatistics) dgReceiveInterval.end();
        if (!p.isControlPacket() && LOG.isLoggable(Level.FINE)) {
            LOG.fine("++ " + p + " queuesize=" + handOffQueue.size());
        }
        handOffQueue.offer(p);
        if (storeStatistics) dgReceiveInterval.begin();
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
    public void receiverAlgorithm() throws InterruptedException, IOException {
        // check ACK timer
        long currentTime = Util.getCurrentTime();
        if (nextACK < currentTime) {
            nextACK = currentTime + ackTimerInterval;
            processACKEvent(true);
        }
        // check NAK timer
        if (nextNAK < currentTime) {
            nextNAK = currentTime + nakTimerInterval;
            processNAKEvent();
        }
        // check EXP timer
        if (nextEXP < currentTime) {
            nextEXP = currentTime + expTimerInterval;
            processEXPEvent();
        }
        // Perform time-bounded UDP receive
        BoltPacket packet = handOffQueue.poll(Util.getSYNTime(), TimeUnit.MICROSECONDS);
        if (packet != null) {
            // Reset exp count to 1
            expCount = 1;
            // If there is no unacknowledged data packet, or if this is an ACK or NAK control packet, reset the EXP timer.
            boolean needEXPReset = false;
            if (packet.isControlPacket()) {
                ControlPacket cp = (ControlPacket) packet;
                int cpType = cp.getControlPacketType();
                if (cpType == ControlPacketType.ACK.getTypeId() || cpType == ControlPacketType.NAK.getTypeId()) {
                    needEXPReset = true;
                }
            }

            if (needEXPReset) {
                nextEXP = Util.getCurrentTime() + expTimerInterval;
            }
            if (storeStatistics) processTime.begin();

            processPacket(packet);

            if (storeStatistics) processTime.end();
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
    protected void processACKEvent(boolean isTriggeredByTimer) throws IOException {
        // 1: Find the sequence number *prior to which* all the packets have been received
        final int ackNumber;
        ReceiverLossListEntry entry = receiverLossList.getFirstEntry();
        if (entry == null) {
            ackNumber = largestReceivedSeqNumber + 1;
        } else {
            ackNumber = entry.getSequenceNumber();
        }
        // 2a: If ackNumber equals to the largest sequence number ever acknowledged by ACK2
        if (ackNumber == largestAcknowledgedAckNumber) {
            //do not send this ACK
            return;
        }
        else if (ackNumber == lastAckNumber) {
            //or it is equals to the ackNumber in the last ACK
            //and the time interval between these two ACK packets
            //is less than 2 RTTs,do not send(stop)
            long timeOfLastSentAck = ackHistoryWindow.getTime(lastAckNumber);
            if (Util.getCurrentTime() - timeOfLastSentAck < 2 * roundTripTime) {
                return;
            }
        }
        final long ackSeqNumber;
        //if this ACK is not triggered by ACK timers,send out a light Ack and stop.
        if (!isTriggeredByTimer) {
            ackSeqNumber = sendLightAcknowledgment(ackNumber);
            return;
        } else {
            //pack the packet speed and link capacity into the ACK packet and send it out.
            //(7).records  the ACK number,ackseqNumber and the departure time of
            //this Ack in the ACK History Window
            ackSeqNumber = sendAcknowledgment(ackNumber);
        }
        AckHistoryEntry sentAckNumber = new AckHistoryEntry(ackSeqNumber, ackNumber, Util.getCurrentTime());
        ackHistoryWindow.add(sentAckNumber);
        //store ack number for next iteration
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
    protected void processNAKEvent() throws IOException {
        final List<Long> seqNumbers = receiverLossList.getFilteredSequenceNumbers(roundTripTime, true);
        sendNAK(seqNumbers);
    }

    /**
     * EXP Event Processing:
     * <ol>
     * <li> Put all the unacknowledged packets into the sender's loss list.
     * <li> If (ExpCount > 16) and at least 3 seconds has elapsed since that
     * last time when ExpCount is reset to 1, or, 3 minutes has elapsed,
     * close the UDT connection and exit.
     * <li> If the sender's loss list is empty, send a keep-alive packet to
     * the peer side.
     * <li> Increase ExpCount by 1.
     * </ol>
     *
     * @throws IOException if shutdown, stop or send of keep-alive fails.
     */
    protected void processEXPEvent() throws IOException {
        if (session.getSocket() == null || !session.getSocket().isActive()) return;
        BoltSender sender = session.getSocket().getSender();
        //put all the unacknowledged packets in the senders loss list
        sender.putUnacknowledgedPacketsIntoLossList();
        if (expCount > 16 && System.currentTimeMillis() - sessionUpSince > IDLE_TIMEOUT) {
            if (!connectionExpiryDisabled && !stopped) {
                sendShutdown();
                stop();
                LOG.info("Session " + session + " expired.");
                return;
            }
        }
        if (!sender.haveLostPackets()) {
            sendKeepAlive();
        }
        expCount++;
    }

    protected void processPacket(BoltPacket p) throws IOException {
        // 3) Check the packet type and process it according to this.

        if (!p.isControlPacket()) {
            DataPacket dp = (DataPacket) p;
            if (storeStatistics) {
                dataPacketInterval.end();
                dataProcessTime.begin();
            }
            onDataPacketReceived(dp);
            if (storeStatistics) {
                dataProcessTime.end();
                dataPacketInterval.begin();
            }
        } else if (p.getControlPacketType() == ControlPacketType.ACK2.getTypeId()) {
            Acknowledgment2 ack2 = (Acknowledgment2) p;
            onAck2PacketReceived(ack2);
        } else if (p instanceof Shutdown) {
            onShutdown();
        }
    }

    protected void onDataPacketReceived(DataPacket dp) throws IOException {
        int currentSequenceNumber = dp.getPacketSequenceNumber();

        //for TESTING : check whether to drop this packet
//		n++;
//		//if(dropRate>0 && n % dropRate == 0){
//			if(n % 1111 == 0){
//				LOG.info("**** TESTING:::: DROPPING PACKET "+currentSequenceNumber+" FOR TESTING");
//				return;
//			}
//		//}
        boolean OK = session.getSocket().haveNewData(dp);
        if (!OK) {
            //need to drop packet...
            return;
        }

        long currentDataPacketArrivalTime = Util.getCurrentTime();

		// 4) If the seqNo of the current data packet is 16n+1, record the time interval
        // between this packet and the last data packet in the packet pair window.
        if ((currentSequenceNumber % 16) == 1 && lastDataPacketArrivalTime > 0) {
            long interval = currentDataPacketArrivalTime - lastDataPacketArrivalTime;
            packetPairWindow.add(interval);
        }

        // 5) Record the packet arrival time in the PKT History Window.
        packetHistoryWindow.add(currentDataPacketArrivalTime);
        // Store current time
        lastDataPacketArrivalTime = currentDataPacketArrivalTime;


        //(6).number of detected lossed packet
        /*(6.a).if the number of the current data packet is greater than LSRN+1,
            put all the sequence numbers between (but excluding) these two values
			into the receiver's loss list and send them to the sender in an NAK packet*/
        if (SequenceNumber.compare(currentSequenceNumber, largestReceivedSeqNumber + 1) > 0) {
            sendNAK(currentSequenceNumber);
        } else if (SequenceNumber.compare(currentSequenceNumber, largestReceivedSeqNumber) < 0) {
                /*(6.b).if the sequence number is less than LRSN,remove it from
                 * the receiver's loss list
				 */
            receiverLossList.remove(currentSequenceNumber);
        }

        statistics.incNumberOfReceivedDataPackets();

        //(7).Update the LRSN
        if (SequenceNumber.compare(currentSequenceNumber, largestReceivedSeqNumber) > 0) {
            largestReceivedSeqNumber = currentSequenceNumber;
        }

        //(8) need to send an ACK? Some cc algorithms use this
        if (ackInterval > 0) {
            if (n % ackInterval == 0) processACKEvent(false);
        }
    }

    /**
     * write a NAK triggered by a received sequence number that is larger than
     * the largestReceivedSeqNumber + 1
     *
     * @param currentSequenceNumber - the currently received sequence number
     * @throws IOException
     */
    protected void sendNAK(long currentSequenceNumber) throws IOException {
        NegativeAcknowledgement nAckPacket = new NegativeAcknowledgement();
        nAckPacket.addLossInfo(largestReceivedSeqNumber + 1, currentSequenceNumber);
        nAckPacket.setSession(session);
        nAckPacket.setDestinationID(session.getDestination().getSocketID());
        //put all the sequence numbers between (but excluding) these two values into the receiver loss list
        for (int i = largestReceivedSeqNumber + 1; i < currentSequenceNumber; i++) {
            final ReceiverLossListEntry detectedLossSeqNumber = new ReceiverLossListEntry(i);
            receiverLossList.insert(detectedLossSeqNumber);
        }
        endpoint.doSend(nAckPacket);
        //LOG.info("NAK for "+currentSequenceNumber);
        statistics.incNumberOfNAKSent();
    }

    protected void sendNAK(List<Long> sequenceNumbers) throws IOException {
        if (sequenceNumbers.isEmpty()) return;
        NegativeAcknowledgement nAckPacket = new NegativeAcknowledgement();
        nAckPacket.addLossInfo(sequenceNumbers);
        nAckPacket.setSession(session);
        nAckPacket.setDestinationID(session.getDestination().getSocketID());
        endpoint.doSend(nAckPacket);
        statistics.incNumberOfNAKSent();
    }

    protected long sendLightAcknowledgment(final int ackNumber) throws IOException {
        Acknowledgement acknowledgmentPkt = buildLightAcknowledgement(ackNumber);
        endpoint.doSend(acknowledgmentPkt);
        statistics.incNumberOfACKSent();
        return acknowledgmentPkt.getAckSequenceNumber();
    }

    protected long sendAcknowledgment(final int ackNumber) throws IOException {
        Acknowledgement acknowledgmentPkt = buildLightAcknowledgement(ackNumber);
        //set the estimate link capacity
        estimateLinkCapacity = packetPairWindow.getEstimatedLinkCapacity();
        acknowledgmentPkt.setEstimatedLinkCapacity(estimateLinkCapacity);
        //set the packet arrival rate
        packetArrivalSpeed = packetHistoryWindow.getPacketArrivalSpeed();
        acknowledgmentPkt.setPacketReceiveRate(packetArrivalSpeed);

        endpoint.doSend(acknowledgmentPkt);

        statistics.incNumberOfACKSent();
        statistics.setPacketArrivalRate(packetArrivalSpeed, estimateLinkCapacity);
        return acknowledgmentPkt.getAckSequenceNumber();
    }

    //builds a "light" Acknowledgement
    private Acknowledgement buildLightAcknowledgement(final int ackNumber) {
        Acknowledgement acknowledgmentPkt = new Acknowledgement();
        //the packet sequence number to which all the packets have been received
        acknowledgmentPkt.setAckNumber(ackNumber);
        //assign this ack a unique increasing ACK sequence number
        acknowledgmentPkt.setAckSequenceNumber(++ackSequenceNumber);
        acknowledgmentPkt.setRoundTripTime(roundTripTime);
        acknowledgmentPkt.setRoundTripTimeVar(roundTripTimeVar);
        //set the buffer size
        acknowledgmentPkt.setBufferSize(bufferSize);

        acknowledgmentPkt.setDestinationID(session.getDestination().getSocketID());
        acknowledgmentPkt.setSession(session);

        return acknowledgmentPkt;
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
    protected void onAck2PacketReceived(Acknowledgment2 ack2) {
        AckHistoryEntry entry = ackHistoryWindow.getEntry(ack2.getAckSequenceNumber());
        if (entry != null) {
            long ackNumber = entry.getAckNumber();
            largestAcknowledgedAckNumber = Math.max(ackNumber, largestAcknowledgedAckNumber);

            long rtt = entry.getAge();
            if (roundTripTime > 0) roundTripTime = (roundTripTime * 7 + rtt) / 8;
            else roundTripTime = rtt;
            roundTripTimeVar = (roundTripTimeVar * 3 + Math.abs(roundTripTimeVar - rtt)) / 4;
            ackTimerInterval = 4 * roundTripTime + roundTripTimeVar + Util.getSYNTime();
            nakTimerInterval = ackTimerInterval;
            statistics.setRTT(roundTripTime, roundTripTimeVar);
        }
    }

    /**
     * On message drop request received:
     * <ol>
     * <li> Tag all packets belong to the message in the receiver buffer so
     * that they will not be read.
     * <li> Remove all corresponding packets in the receiver's loss list.
     * </ol>
     *
     * @param messageDropRequest the received MessageDropRequest packet.
     */
    protected void onMessageDropRequest(MessageDropRequest messageDropRequest) {
        //TODO this was never implemented. Investigate if required and implications.
    }

    protected void sendKeepAlive() throws IOException {
        KeepAlive ka = new KeepAlive();
        ka.setDestinationID(session.getDestination().getSocketID());
        ka.setSession(session);
        endpoint.doSend(ka);
    }

    protected void sendShutdown() throws IOException {
        Shutdown s = new Shutdown();
        s.setDestinationID(session.getDestination().getSocketID());
        s.setSession(session);
        endpoint.doSend(s);
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

    protected void onShutdown() throws IOException {
        stop();
    }

    public void stop() throws IOException {
        stopped = true;
        session.getSocket().close();
        // Stop our sender as well.
        session.getSocket().getSender().stop();
    }

    public String toString() {
        return "BoltReceiver " + session + "\n" +
                "LossList: " + receiverLossList;
    }

}
