package io.lyracommunity.bolt.api;

import io.lyracommunity.bolt.util.Util;

import java.net.InetAddress;

/**
 * Created by omahoc9 on 3/11/16.
 */
public class Config
{

    private volatile float packetDropRate;

    /** Simulated network latency, in milliseconds. */
    private int simulatedLatency;

    /** Simulated network jitter, in milliseconds. */
    private int simulatedMaxJitter;

    /** Simulated network bandwidth, in KB/sec. */
    private int simulatedBandwidth;

    private InetAddress localAddress;

    private int localPort;

    private boolean allowSessionExpiry = true;

    /**
     * The consecutive number of EXP events before the session expires.
     */
    private int expLimit = 16;

    /**
     * If larger than 0, the receiver should acknowledge every n'th packet.
     */
    private int ackInterval = 16;

    public static final int DEFAULT_DATAGRAM_SIZE = 1400;

    private final int datagramSize = 1400;

    /**
     * Microseconds to next EXP event. Default to 500 millis.
     */
    private long expTimerInterval = 50 * Util.getSYNTime();

    /** Max ACK timer interval, in microseconds. */
    private long maxAckTimerInterval = 10 * Util.getSYNTime();

    /** Whether to collect more detailed statistics or not. */
    private boolean deepStatistics = true;

    /**
     * Create a new instance.
     *
     * @param localAddress local address to bind to. null for default network interface of machine.
     * @param localPort port to bind to. If 0, an ephemeral port is chosen.
     */
    public Config(final InetAddress localAddress, final int localPort) {
        this.localAddress = localAddress;
        this.localPort = localPort;
    }

    public InetAddress getLocalAddress() {
        return localAddress;
    }

    public int getLocalPort() {
        return localPort;
    }

    /**
     * Get the rate at which packets should be dropped.
     *
     * @return the rate. Example: 3.5 means every 3.5 packets should be dropped.
     */
    public float getPacketDropRate() {
        return packetDropRate;
    }

    /**
     * Set an artificial packet loss.
     *
     * @param packetLossPercentage the packet loss as a percentage (ie. x, where 0 <= x <= 1.0).
     */
    public Config setPacketLoss(final float packetLossPercentage) {
        final float normalizedPacketLossPercentage = Math.min(packetLossPercentage, 1f);
        packetDropRate = (normalizedPacketLossPercentage <= 0f) ? 0f : 1f / normalizedPacketLossPercentage;
        return this;
    }

    public boolean isAllowSessionExpiry() {
        return allowSessionExpiry;
    }

    public void setAllowSessionExpiry(final boolean allowSessionExpiry) {
        this.allowSessionExpiry = allowSessionExpiry;
    }

    /**
     * Get the ACK interval. If larger than 0, the receiver should acknowledge
     * every n'th packet.
     */
    public int getAckInterval() {
        return ackInterval;
    }

    /**
     * Set the ACK interval. If larger than 0, the receiver should acknowledge
     * every n'th packet.
     */
    public void setAckInterval(int ackInterval) {
        this.ackInterval = ackInterval;
    }

    public int getDatagramSize() {
        return datagramSize;
    }

    public void setExpLimit(final int expLimit)
    {
        if (expLimit <= 1) throw new IllegalArgumentException("expLimit must be at least 2 or greater");
        this.expLimit = expLimit;
    }

    public int getExpLimit() {
        return expLimit;
    }

    public long getExpTimerInterval()
    {
        return expTimerInterval;
    }

    public void setExpTimerInterval(long expTimerInterval)
    {
        this.expTimerInterval = expTimerInterval;
    }

    public long getMaxAckTimerInterval() {
        return maxAckTimerInterval;
    }

    public void setMaxAckTimerInterval(long maxAckTimerInterval) {
        this.maxAckTimerInterval = maxAckTimerInterval;
    }

    public int getSimulatedLatency() {
        return simulatedLatency;
    }

    public void setSimulatedLatency(final int simulatedLatency) {
        this.simulatedLatency = simulatedLatency;
    }

    public int getSimulatedMaxJitter() {
        return simulatedMaxJitter;
    }

    public void setSimulatedMaxJitter(final int simulatedMaxJitter) {
        this.simulatedMaxJitter = simulatedMaxJitter;
    }

    public int getSimulatedBandwidthInBytesPerSecond() {
        return simulatedBandwidth * 1024;
    }

    public int getSimulatedBandwidth() {
        return simulatedBandwidth;
    }

    public void setSimulatedBandwidth(final int simulatedBandwidth) {
        this.simulatedBandwidth = simulatedBandwidth;
    }

    public boolean isDeepStatistics() {
        return deepStatistics;
    }

    public void setDeepStatistics(boolean deepStatistics) {
        this.deepStatistics = deepStatistics;
    }

    public void setLocalPort(final int localPort) {
        this.localPort = localPort;
    }

}
