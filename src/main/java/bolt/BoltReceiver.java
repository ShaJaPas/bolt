package bolt;

import bolt.packets.*;
import bolt.packets.ControlPacket.ControlPacketType;
import bolt.packets.Shutdown;
import bolt.receiver.*;
import bolt.statistic.MeanValue;
import bolt.statistic.BoltStatistics;
import bolt.util.SequenceNumber;
import bolt.util.BoltThreadFactory;
import bolt.util.Util;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * receiver part of a Bolt entity
 *
 * @see BoltSender
 */
public class BoltReceiver {

    private static final Logger logger = Logger.getLogger(BoltReceiver.class.getName());
    /**
     * if set to true connections will not expire, but will only be
     * closed by a Shutdown message
     */
    public static boolean connectionExpiryDisabled = false;
    //every nth packet will be discarded... for testing only of course
    public static int dropRate = 0;
    private final BoltEndPoint endpoint;
    private final BoltSession session;
    private final BoltStatistics statistics;
    //record seqNo of detected lostdata and latest feedback time
    private final ReceiverLossList receiverLossList;
    //record each sent ACK and the sent time
    private final AckHistoryWindow ackHistoryWindow;
    //Packet history window that stores the time interval between the current and the last seq.
    private final PacketHistoryWindow packetHistoryWindow;

    //ACK event related
    /*records the time interval between each probing pair
    compute the median packet pair interval of the last
    16 packet pair intervals (PI) and the estimate link capacity.(packet/s)*/
    private final PacketPairWindow packetPairWindow;
    //instant when the session was created (for expiry checking)
    private final long sessionUpSince;

    //EXP event related
    //milliseconds to timeout a new session that stays idle
    private final long IDLE_TIMEOUT = 3 * 60 * 1000;
    //buffer size for storing data
    private final long bufferSize;
    //stores received packets to be sent
    private final BlockingQueue<BoltPacket> handoffQueue;
    private final boolean storeStatistics;
    //estimated link capacity
    long estimateLinkCapacity;
    // the packet arrival rate
    long packetArrivalSpeed;
    //round trip time, calculated from ACK/ACK2 pairs
    long roundTripTime = 0;
    //round trip time variance
    long roundTripTimeVar = roundTripTime / 2;
    //for storing the arrival time of the last received data packet
    private volatile long lastDataPacketArrivalTime = 0;
    //largest received data packet sequence number(LRSN)
    private volatile long largestReceivedSeqNumber = 0;
    //last Ack number
    private long lastAckNumber = 0;
    //largest Ack number ever acknowledged by ACK2
    private volatile long largestAcknowledgedAckNumber = -1;
    //a variable to record number of continuous EXP time-out events
    private volatile long expCount = 0;
    //to check the ACK, NAK, or EXP timer
    private long nextACK;
    //microseconds to next ACK event
    private long ackTimerInterval = Util.getSYNTime();
    private long nextNAK;
    //microseconds to next NAK event
    private long nakTimerInterval = Util.getSYNTime();
    private long nextEXP;
    //microseconds to next EXP event
    private long expTimerInterval = 100 * Util.getSYNTime();
    private Thread receiverThread;
    private volatile boolean stopped = false;
    //(optional) ack interval (see CongestionControl interface)
    private volatile long ackInterval = -1;
    private MeanValue dgReceiveInterval;
    private MeanValue dataPacketInterval;
    private MeanValue processTime;
    private MeanValue dataProcessTime;
    //number of received data packets
    private int n = 0;
    private volatile long ackSequenceNumber = 0;

    /**
     * create a receiver with a valid {@link BoltSession}
     *
     * @param session
     */
    public BoltReceiver(BoltSession session, BoltEndPoint endpoint) {
        this.endpoint = endpoint;
        this.session = session;
        this.sessionUpSince = System.currentTimeMillis();
        this.statistics = session.getStatistics();
        if (!session.isReady()) throw new IllegalStateException("BoltSession is not ready.");
        ackHistoryWindow = new AckHistoryWindow(16);
        packetHistoryWindow = new PacketHistoryWindow(16);
        receiverLossList = new ReceiverLossList();
        packetPairWindow = new PacketPairWindow(16);
        largestReceivedSeqNumber = session.getInitialSequenceNumber() - 1;
        bufferSize = session.getReceiveBufferSize();
        handoffQueue = new ArrayBlockingQueue<>(4 * session.getFlowWindowSize());
        storeStatistics = Boolean.getBoolean("bolt.receiver.storeStatistics");
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

    //starts the sender algorithm
    private void start() {

        Runnable r = () -> {
            try {
                while (session.getSocket() == null) Thread.sleep(100);
                session.getSocket().getInputStream();

                logger.info("STARTING RECEIVER for " + session);
                nextACK = Util.getCurrentTime() + ackTimerInterval;
                nextNAK = (long) (Util.getCurrentTime() + 1.5 * nakTimerInterval);
                nextEXP = Util.getCurrentTime() + 2 * expTimerInterval;
                ackInterval = session.getCongestionControl().getAckInterval();
                while (!stopped) {
                    receiverAlgorithm();
                }
            } catch (Exception ex) {
                logger.log(Level.SEVERE, "", ex);
            }
            logger.info("STOPPING RECEIVER for " + session);
        };
        receiverThread = BoltThreadFactory.get().newThread(r);
        String s = (session instanceof ServerSession) ? "ServerSession" : "ClientSession";
        receiverThread.setName("BoltReceiver-" + s + "-" + receiverThread.getName());
        receiverThread.start();
    }

    /*
     * packets are written by the endpoint
     */
    protected void receive(BoltPacket p) throws IOException {
        if (storeStatistics) dgReceiveInterval.end();
        if (!p.isControlPacket() && logger.isLoggable(Level.FINE)) {
            logger.fine("++ " + p + " queuesize=" + handoffQueue.size());
        }
        handoffQueue.offer(p);
        if (storeStatistics) dgReceiveInterval.begin();
    }

    /**
     * receiver algorithm
     * see specification P11.
     */
    public void receiverAlgorithm() throws InterruptedException, IOException {
        //check ACK timer
        long currentTime = Util.getCurrentTime();
        if (nextACK < currentTime) {
            nextACK = currentTime + ackTimerInterval;
            processACKEvent(true);
        }
        //check NAK timer
        if (nextNAK < currentTime) {
            nextNAK = currentTime + nakTimerInterval;
            processNAKEvent();
        }

        //check EXP timer
        if (nextEXP < currentTime) {
            nextEXP = currentTime + expTimerInterval;
            processEXPEvent();
        }
        //perform time-bounded UDP receive
        BoltPacket packet = handoffQueue.poll(Util.getSYNTime(), TimeUnit.MICROSECONDS);
        if (packet != null) {
            //reset exp count to 1
            expCount = 1;
            //If there is no unacknowledged data packet, or if this is an
            //ACK or NAK control packet, reset the EXP timer.
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

        Thread.yield();
    }

    /**
     * process ACK event (see spec. p 12)
     */
    protected void processACKEvent(boolean isTriggeredByTimer) throws IOException {
        //(1).Find the sequence number *prior to which* all the packets have been received
        final long ackNumber;
        ReceiverLossListEntry entry = receiverLossList.getFirstEntry();
        if (entry == null) {
            ackNumber = largestReceivedSeqNumber + 1;
        } else {
            ackNumber = entry.getSequenceNumber();
        }
        //(2).a) if ackNumber equals to the largest sequence number ever acknowledged by ACK2
        if (ackNumber == largestAcknowledgedAckNumber) {
            //do not send this ACK
            return;
        } else if (ackNumber == lastAckNumber) {
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
     * process NAK event (see spec. p 13)
     */
    protected void processNAKEvent() throws IOException {
        //find out all sequence numbers whose last feedback time larger than is k*RTT
        List<Long> seqNumbers = receiverLossList.getFilteredSequenceNumbers(roundTripTime, true);
        sendNAK(seqNumbers);
    }

    /**
     * process EXP event (see spec. p 13)
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
                logger.info("Session " + session + " expired.");
                return;
            }
        }
        if (!sender.haveLostPackets()) {
            sendKeepAlive();
        }
        expCount++;
    }

    protected void processPacket(BoltPacket p) throws IOException {
        //(3).Check the packet type and process it according to this.

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
        long currentSequenceNumber = dp.getPacketSequenceNumber();

        //for TESTING : check whether to drop this packet
//		n++;
//		//if(dropRate>0 && n % dropRate == 0){
//			if(n % 1111 == 0){
//				logger.info("**** TESTING:::: DROPPING PACKET "+currentSequenceNumber+" FOR TESTING");
//				return;
//			}
//		//}
        boolean OK = session.getSocket().getInputStream().haveNewData(currentSequenceNumber, dp.getData());
        if (!OK) {
            //need to drop packet...
            return;
        }

        long currentDataPacketArrivalTime = Util.getCurrentTime();

		/*(4).if the seqNo of the current data packet is 16n+1,record the
        time interval between this packet and the last data packet
		in the packet pair window*/
        if ((currentSequenceNumber % 16) == 1 && lastDataPacketArrivalTime > 0) {
            long interval = currentDataPacketArrivalTime - lastDataPacketArrivalTime;
            packetPairWindow.add(interval);
        }

        //(5).record the packet arrival time in the PKT History Window.
        packetHistoryWindow.add(currentDataPacketArrivalTime);


        //store current time
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
        for (long i = largestReceivedSeqNumber + 1; i < currentSequenceNumber; i++) {
            ReceiverLossListEntry detectedLossSeqNumber = new ReceiverLossListEntry(i);
            receiverLossList.insert(detectedLossSeqNumber);
        }
        endpoint.doSend(nAckPacket);
        //logger.info("NAK for "+currentSequenceNumber);
        statistics.incNumberOfNAKSent();
    }

    protected void sendNAK(List<Long> sequenceNumbers) throws IOException {
        if (sequenceNumbers.size() == 0) return;
        NegativeAcknowledgement nAckPacket = new NegativeAcknowledgement();
        nAckPacket.addLossInfo(sequenceNumbers);
        nAckPacket.setSession(session);
        nAckPacket.setDestinationID(session.getDestination().getSocketID());
        endpoint.doSend(nAckPacket);
        statistics.incNumberOfNAKSent();
    }

    protected long sendLightAcknowledgment(long ackNumber) throws IOException {
        Acknowledgement acknowledgmentPkt = buildLightAcknowledgement(ackNumber);
        endpoint.doSend(acknowledgmentPkt);
        statistics.incNumberOfACKSent();
        return acknowledgmentPkt.getAckSequenceNumber();
    }

    protected long sendAcknowledgment(long ackNumber) throws IOException {
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
    private Acknowledgement buildLightAcknowledgement(long ackNumber) {
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
     * spec p. 13: <br/>
     * 1) Locate the related ACK in the ACK History Window according to the
     * ACK sequence number in this ACK2.  <br/>
     * 2) Update the largest ACK number ever been acknowledged. <br/>
     * 3) Calculate new rtt according to the ACK2 arrival time and the ACK
     * departure time, and update the RTT value as: RTT = (RTT * 7 +
     * rtt) / 8.  <br/>
     * 4) Update RTTVar by: RTTVar = (RTTVar * 3 + abs(RTT - rtt)) / 4.  <br/>
     * 5) Update both ACK and NAK period to 4 * RTT + RTTVar + SYN.  <br/>
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
        //stop our sender as well
        session.getSocket().getSender().stop();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BoltReceiver ").append(session).append("\n");
        sb.append("LossList: " + receiverLossList);
        return sb.toString();
    }

}
