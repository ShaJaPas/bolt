package io.lyracommunity.bolt;

import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.session.SessionState;
import io.lyracommunity.bolt.statistic.BoltStatistics;
import io.lyracommunity.bolt.util.SeqNum;
import io.lyracommunity.bolt.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.IntStream;


/**
 * Default Bolt congestion control.
 * <p>
 * Uses AIMD (Additive increase, multiplicative decrease).
 */
public class BoltCongestionControl implements CongestionControl {

    private static final Logger LOG         = LoggerFactory.getLogger(BoltCongestionControl.class);
    private static final long   PS          = Config.DEFAULT_DATAGRAM_SIZE;
    private static final double BETA_DIV_PS = 0.0000015 / PS;

    private final SessionState sessionState;

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
    private double congestionWindowSize;

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
    private long averageNakNum;

    /**
     * If in slow start phase.
     */
    private boolean slowStartPhase = true;

    /**
     * Last ACKed seq no.
     */
    private long lastAckSeqNumber = -1;

    /**
     * Max reliability sequence number, sent out when last decrease happened.
     */
    private int lastDecreaseRelSeqNo;

    /**
     * NAK counter.
     */
    private long nakCount = 1;

    /**
     * This flag avoids immediate rate increase after a NAK.
     */
    private boolean loss = false;

    public BoltCongestionControl(final SessionState sessionState, final BoltStatistics statistics,
                                 final double initialCongestionWindowSize) {
        this.sessionState = sessionState;
        this.statistics = statistics;
        this.congestionWindowSize = initialCongestionWindowSize;
        this.lastDecreaseRelSeqNo = 0;
    }

    @Override
    public void init() {
        // Do nothing.
    }

    @Override
    public void setRTT(final long rtt, final long rttVar) {
        this.roundTripTime = rtt;
    }

    @Override
    public void updatePacketArrivalRate(long rate, long linkCapacity) {
        packetArrivalRate = (packetArrivalRate > 0)
                ? (packetArrivalRate * 7 + rate) / 8
                : rate;

        estimatedLinkCapacity = (estimatedLinkCapacity > 0)
                ? (estimatedLinkCapacity * 7 + linkCapacity) / 8
                : linkCapacity;
    }

    @Override
    public long getPacketArrivalRate() {
        return packetArrivalRate;
    }

    @Override
    public long getEstimatedLinkCapacity() {
        return estimatedLinkCapacity;
    }

    @Override
    public double getSendInterval() {
        return packetSendingPeriod;
    }

    /**
     * Get the congestion window size.
     *
     * @return the congestion window size.
     */
    @Override
    public double getCongestionWindowSize() {
        return congestionWindowSize;
    }

    /**
     * @see CongestionControl#onACK(long)
     */
    @Override
    public void onACK(final long ackSeqNum) {
        // Increase window during slow start.
        if (slowStartPhase) {
            congestionWindowSize += ackSeqNum - lastAckSeqNumber;
            lastAckSeqNumber = ackSeqNum;
            // But not beyond a maximum size.
            if (congestionWindowSize > sessionState.getFlowWindowSize()) {
                slowStartPhase = false;
                packetSendingPeriod = (packetArrivalRate > 0)
                        ? 1000000.0 / packetArrivalRate
                        : congestionWindowSize / (roundTripTime + Util.getSYNTimeD());
            }

        }
        else {
            // 1. If not in slow start phase, set congestion window size to product of packet arrival rate and (rtt + SYN)
            final double A = (packetArrivalRate / 1_000_000d) * (roundTripTime + Util.getSYNTimeD());
            congestionWindowSize = (long) A + 16;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Receive rate [{}]  RTT [{}]  Set to window size [{}]", packetArrivalRate, roundTripTime, (A + 16));
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
        final double numOfIncreasingPacket = computeNumOfIncreasingPacket();

        // 5) Update the send period
        final double factor = Util.getSYNTimeD() / (packetSendingPeriod * numOfIncreasingPacket + Util.getSYNTimeD());
        packetSendingPeriod = factor * packetSendingPeriod;
        // packetSendingPeriod=0.995*packetSendingPeriod;

        statistics.setSendPeriod(packetSendingPeriod);
    }

    private double computeNumOfIncreasingPacket() {
        // Difference between link capacity and sending speed, in packets per second.
        final double remaining = estimatedLinkCapacity - 1_000_000d / packetSendingPeriod;

        if (remaining <= 0) {
            return 1.0 / Config.DEFAULT_DATAGRAM_SIZE;
        }
        else {
            double exp = Math.ceil(Math.log10(remaining * PS * 8));
            double power10 = Math.pow(10.0, exp) * BETA_DIV_PS;
            return Math.max(power10, 1 / PS);
        }
    }

    @Override
    public void onLoss(final IntStream lossInfo, final int currentMaxRelSeqNum) {
        loss = true;
        final int firstBiggestLossRelSeqNo = lossInfo.findFirst()
                .orElseThrow(() -> new IllegalStateException("Loss info was empty"));
        nakCount++;
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

        // 2) If this NAK starts a new congestion epoch
        if (SeqNum.compare16(firstBiggestLossRelSeqNo, lastDecreaseRelSeqNo) > 0) {
            // Increase inter-packet interval.
            packetSendingPeriod = Math.ceil(packetSendingPeriod * 1.125);
            // Update AvgNakNum (the average number of NAKs per congestion).
            averageNakNum = (int) Math.ceil(averageNakNum * 0.875 + nakCount * 0.125);
            // Reset NAKCount and DecCount to 1,
            nakCount = 1;
            decCount = 1;
            // Compute decrease random to a random (average distribution) number between 1 and AvgNakNum.
            decreaseRandom = (int) Math.ceil((averageNakNum - 1) * Math.random() + 1);
            // Update last decrease sequence number.
            lastDecreaseRelSeqNo = currentMaxRelSeqNum;
            // Stop.
        }
        // 3) If DecCount <= 5, and NAKCount == DecCount * DecRandom:
        else if (decCount <= 5 && nakCount == decCount * decreaseRandom) {
            // a. Update SND period: SND = SND * 1.125;
            packetSendingPeriod = Math.ceil(packetSendingPeriod * 1.125);
            // b. Increase DecCount by 1;
            decCount++;
            // c. Record the current largest sent sequence number (LastDecSeq).
            lastDecreaseRelSeqNo = currentMaxRelSeqNum;
        }

        statistics.setSendPeriod(packetSendingPeriod);
    }

    @Override
    public void close() {
    }

}
