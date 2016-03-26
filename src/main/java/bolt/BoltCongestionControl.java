package bolt;

import bolt.statistic.BoltStatistics;
import bolt.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * Default Bolt congestion control.
 * <p>
 * The algorithm is adapted from the C++ reference implementation.
 */
public class BoltCongestionControl implements CongestionControl {

    private static final Logger LOG         = LoggerFactory.getLogger(BoltCongestionControl.class);
    private static final long   PS          = BoltEndPoint.DATAGRAM_SIZE;
    private static final double BETA_DIV_PS = 0.0000015 / PS;

    protected final BoltSession session;

    protected final BoltStatistics statistics;

    /**
     * Round trip time in microseconds.
     */
    protected long roundTripTime = 0;

    /**
     * Rate in packets per second.
     */
    protected long packetArrivalRate = 0;

    /**
     * Link capacity in packets per second.
     */
    protected long estimatedLinkCapacity = 0;

    /**
     * Packet sending period = packet send interval, in microseconds.
     */
    protected double packetSendingPeriod = 1;

    /**
     * Congestion window size, in packets.
     */
    protected double congestionWindowSize = 16;

    /**
     * If larger than 0, the receiver should acknowledge every n'th packet.
     */
    protected long ackInterval = -1;

    /**
     * Number of decreases in a congestion epoch.
     */
    long decCount = 1;

    /**
     * Random threshold on decrease by number of loss events.
     */
    long decreaseRandom = 1;

    /**
     * Average number of NAKs per congestion.
     */
    long averageNACKNum;

    /**
     * If in slow start phase.
     */
    private boolean slowStartPhase = true;

    /**
     * Last ACKed seq no.
     */
    private long lastAckSeqNumber = -1;

    /**
     * Max packet seq. no. sent out when last decrease happened.
     */
    private long lastDecreaseSeqNo;

    /**
     * NAK counter.
     */
    private long nACKCount = 1;

    /**
     * This flag avoids immediate rate increase after a NAK.
     */
    private boolean loss = false;


    public BoltCongestionControl(BoltSession session) {
        this.session = session;
        this.statistics = session.getStatistics();
        this.lastDecreaseSeqNo = session.getInitialSequenceNumber() - 1;
    }

    public void init() {
        // Do nothing.
    }

    public void setRTT(long rtt, long rttVar) {
        this.roundTripTime = rtt;
    }

    public void updatePacketArrivalRate(long rate, long linkCapacity) {
        // See spec p. 14.
        if (packetArrivalRate > 0) packetArrivalRate = (packetArrivalRate * 7 + rate) / 8;
        else packetArrivalRate = rate;
        if (estimatedLinkCapacity > 0) estimatedLinkCapacity = (estimatedLinkCapacity * 7 + linkCapacity) / 8;
        else estimatedLinkCapacity = linkCapacity;
    }

    public long getPacketArrivalRate() {
        return packetArrivalRate;
    }

    public long getEstimatedLinkCapacity() {
        return estimatedLinkCapacity;
    }

    public double getSendInterval() {
        return packetSendingPeriod;
    }

    public long getAckInterval() {
        return ackInterval;
    }

    public void setAckInterval(long ackInterval) {
        this.ackInterval = ackInterval;
        if (session.getSocket() != null && session.getSocket().getReceiver() != null) {
            session.getSocket().getReceiver().setAckInterval(ackInterval);
        }
    }

    /**
     * Get the congestion window size.
     *
     * @return the congestion window size.
     */
    public double getCongestionWindowSize() {
        return congestionWindowSize;
    }

    /**
     * @see bolt.CongestionControl#onACK(long)
     */
    public void onACK(final long ackSeqNo) {
        // increase window during slow start
        if (slowStartPhase) {
            congestionWindowSize += ackSeqNo - lastAckSeqNumber;
            lastAckSeqNumber = ackSeqNo;
            // but not beyond a maximum size
            if (congestionWindowSize > session.getFlowWindowSize()) {
                slowStartPhase = false;
                if (packetArrivalRate > 0) {
                    packetSendingPeriod = 1000000.0 / packetArrivalRate;
                }
                else {
                    packetSendingPeriod = congestionWindowSize / (roundTripTime + Util.getSYNTimeD());
                }
            }

        }
        else {
            // 1. if it's not in slow start phase,set the congestion window size to the product of packet arrival rate and(rtt +SYN)
            double A = packetArrivalRate / 1000000.0 * (roundTripTime + Util.getSYNTimeD());
            congestionWindowSize = (long) A + 16;
            if (LOG.isTraceEnabled()) {
                LOG.trace("receive rate " + packetArrivalRate + " rtt " + roundTripTime + " set to window size: " + (A + 16));
            }
        }

        // no rate increase during slow start
        if (slowStartPhase) return;

        // No rate increase "immediately" after a NAK
        if (loss) {
            loss = false;
            return;
        }

        // 4) compute the increase in sent packets for the next SYN period
        double numOfIncreasingPacket = computeNumOfIncreasingPacket();

        // 5) update the send period
        double factor = Util.getSYNTimeD() / (packetSendingPeriod * numOfIncreasingPacket + Util.getSYNTimeD());
        packetSendingPeriod = factor * packetSendingPeriod;
        // packetSendingPeriod=0.995*packetSendingPeriod;

        statistics.setSendPeriod(packetSendingPeriod);
    }

    /**
     * See spec page 16.
     */
    private double computeNumOfIncreasingPacket() {
        // Difference between link capacity and sending speed, in packets per second.
        final double remaining = estimatedLinkCapacity - 1000000.0 / packetSendingPeriod;

        if (remaining <= 0) {
            return 1.0 / BoltEndPoint.DATAGRAM_SIZE;
        }
        else {
            double exp = Math.ceil(Math.log10(remaining * PS * 8));
            double power10 = Math.pow(10.0, exp) * BETA_DIV_PS;
            return Math.max(power10, 1 / PS);
        }
    }

    public void onLoss(final List<Integer> lossInfo) {
        loss = true;
        long firstBiggestLossSeqNo = lossInfo.get(0);
        nACKCount++;
        // 1) If it is in slow start phase, set inter-packet interval to 1/recvrate. Slow start ends. Stop.
        if (slowStartPhase) {
            if (packetArrivalRate > 0) {
                packetSendingPeriod = 100000.0 / packetArrivalRate;
            }
            else {
                packetSendingPeriod = congestionWindowSize / (roundTripTime + Util.getSYNTime());
            }
            slowStartPhase = false;
            return;
        }

        // TODO may need to change currentSeqNo to currentReliabilitySeqNo and according logic
        long currentMaxSequenceNumber = session.getSocket().getSender().getCurrentSequenceNumber();
        // 2) If this NAK starts a new congestion epoch
        if (firstBiggestLossSeqNo > lastDecreaseSeqNo) {
            // -increase inter-packet interval
            packetSendingPeriod = Math.ceil(packetSendingPeriod * 1.125);
            // -Update AvgNAKNum(the average number of NAKs per congestion)
            averageNACKNum = (int) Math.ceil(averageNACKNum * 0.875 + nACKCount * 0.125);
            // -reset NAKCount and DecCount to 1,
            nACKCount = 1;
            decCount = 1;
            // - compute DecRandom to a random (average distribution) number between 1 and AvgNAKNum
            decreaseRandom = (int) Math.ceil((averageNACKNum - 1) * Math.random() + 1);
            // -Update LastDecSeq
            lastDecreaseSeqNo = currentMaxSequenceNumber;
            // -Stop.
        }
        // 3) If DecCount <= 5, and NAKCount == DecCount * DecRandom:
        else if (decCount <= 5 && nACKCount == decCount * decreaseRandom) {
            // a. Update SND period: SND = SND * 1.125;
            packetSendingPeriod = Math.ceil(packetSendingPeriod * 1.125);
            // b. Increase DecCount by 1;
            decCount++;
            // c. Record the current largest sent sequence number (LastDecSeq).
            lastDecreaseSeqNo = currentMaxSequenceNumber;
        }

        statistics.setSendPeriod(packetSendingPeriod);
    }

    public void onTimeout() {
    }

    public void onPacketSend(long packetSeqNo) {
    }

    public void onPacketReceive(long packetSeqNo) {
    }

    public void close() {
    }


}
