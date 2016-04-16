package io.lyracommunity.bolt;

import java.util.List;

/**
 * Congestion control interface.
 */
public interface CongestionControl {

    /**
     * Callback function to be called (only) at the start of a Bolt connection.
     * when the Bolt socket is connected.
     */
    void init();

    /**
     * Set round-trip time and associated variance.
     *
     * @param rtt    round trip time, in microseconds.
     * @param rttVar round trip time variance, in microseconds.
     */
    void setRTT(long rtt, long rttVar);

    /**
     * update packet arrival rate and link capacity with the
     * values received in an ACK packet
     *
     * @param rate         - packet rate in packets per second
     * @param linkCapacity - estimated link capacity in packets per second
     */
    void updatePacketArrivalRate(long rate, long linkCapacity);

    /**
     * @return the current value of the packet arrival.
     */
    long getPacketArrivalRate();

    /**
     * @return the current value of the estimated link capacity.
     */
    long getEstimatedLinkCapacity();

    /**
     * get the current value of the inter-packet interval in microseconds
     */
    double getSendInterval();

    /**
     * get the congestion window size
     */
    double getCongestionWindowSize();

    /**
     * Callback function to be called when an ACK packet is received.
     *
     * @param ackSeqNum the data sequence number acknowledged by this ACK.
     */
    void onACK(long ackSeqNum);

    /**
     * Callback function to be called when a loss report is received.
     *
     * @param lossInfo            list of sequence number of packets.
     * @param currentMaxRelSeqNum the current highest reliability seq num.
     */
    void onLoss(List<Integer> lossInfo, int currentMaxRelSeqNum);

    /**
     * Callback function to be called when a Bolt connection is closed.
     */
    void close();

}