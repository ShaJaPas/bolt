package io.lyracommunity.bolt;

import io.lyracommunity.bolt.statistic.BoltStatistics;
import io.lyracommunity.bolt.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * Default Bolt congestion control.
 * <p>
 * The algorithm is adapted from the C++ reference implementation.
 */
class BoltCongestionControl implements CongestionControl {

    private static final Logger LOG         = LoggerFactory.getLogger(BoltCongestionControl.class);
    private static final long   PS          = BoltEndPoint.DATAGRAM_SIZE;
    private static final double BETA_DIV_PS = 0.0000015 / PS;

    protected final BoltSession session;

    private final BoltStatistics statistics;

    /**
     * Round trip time in microseconds.
     */
    private long roundTripTime = 0;

    /**
     * Rate in packets per second.
     */
    private long packetArrivalRate = 0;

    /**
     * Link capacity in packets per second.
     */
    private long estimatedLinkCapacity = 0;

    /**
     * Packet sending period = packet send interval, in microseconds.
     */
    private double packetSendingPeriod = 1;

    /**
     * Congestion window size, in packets.
     */
    private double congestionWindowSize = 16;

    /**
     * Number of decreases in a congestion epoch.
     */
    private long decCount = 1;

    /**
     * Random threshold on decrease by number of loss events.
     */
    private long decreaseRandom = 1;

    /**
     * Average number of NAKs per congestion.
     */
    private long averageNACKNum;

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


    BoltCongestionControl(final BoltSession session) {
        this.session = session;
        this.statistics = session.getStatistics();
        this.lastDecreaseSeqNo = session.getInitialSequenceNumber() - 1;
    }

    public void init() {
        // Do nothing.
    }

    public void setRTT(final long rtt, final long rttVar) {
        this.roundTripTime = rtt;
    }

    public void updatePacketArrivalRate(long rate, long linkCapacity) {
        packetArrivalRate = (packetArrivalRate > 0)
                ? (packetArrivalRate * 7 + rate) / 8
                : rate;

        estimatedLinkCapacity = (estimatedLinkCapacity > 0)
                ? (estimatedLinkCapacity * 7 + linkCapacity) / 8
                : linkCapacity;
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

    /**
     * Get the congestion window size.
     *
     * @return the congestion window size.
     */
    public double getCongestionWindowSize() {
        return congestionWindowSize;
    }

    /**
     * @see CongestionControl#onACK(long)
     */
    public void onACK(final long ackSeqNo) {
        // Increase window during slow start.
        if (slowStartPhase) {
            congestionWindowSize += ackSeqNo - lastAckSeqNumber;
            lastAckSeqNumber = ackSeqNo;
            // But not beyond a maximum size.
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
            final double A = packetArrivalRate / 1_000_000d * (roundTripTime + Util.getSYNTimeD());
            congestionWindowSize = (long) A + 16;
            if (LOG.isTraceEnabled()) {
                LOG.trace("Receive rate [{}]  RTT [{}]  Set to window size [{}]", packetArrivalRate, roundTripTime, (A + 16));
            }
        }

        // No rate increase during slow start
        if (slowStartPhase) return;

        // No rate increase "immediately" after a NAK
        if (loss) {
            loss = false;
            return;
        }

        // 4) Compute the increase in sent packets for the next SYN period
        double numOfIncreasingPacket = computeNumOfIncreasingPacket();

        // 5) Update the send period
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
        final double remaining = estimatedLinkCapacity - 1_000_000d / packetSendingPeriod;

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

        // TODO make sure this is valid
        long currentMaxRelSequenceNumber = session.getSocket().getSender().getCurrentReliabilitySequenceNumber();
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
            lastDecreaseSeqNo = currentMaxRelSequenceNumber;
            // -Stop.
        }
        // 3) If DecCount <= 5, and NAKCount == DecCount * DecRandom:
        else if (decCount <= 5 && nACKCount == decCount * decreaseRandom) {
            // a. Update SND period: SND = SND * 1.125;
            packetSendingPeriod = Math.ceil(packetSendingPeriod * 1.125);
            // b. Increase DecCount by 1;
            decCount++;
            // c. Record the current largest sent sequence number (LastDecSeq).
            lastDecreaseSeqNo = currentMaxRelSequenceNumber;
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
