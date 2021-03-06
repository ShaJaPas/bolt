package io.lyracommunity.bolt.api;

import io.lyracommunity.bolt.util.Util;

import java.net.InetAddress;

/**
 * Bolt configuration class.
 *
 * @author Cian.
 */
public class Config {

    public static final int DEFAULT_DATAGRAM_SIZE = 1400;
    private final       int datagramSize          = 1400;
    private volatile float       packetDropRate;
    /**
     * Simulated network latency, in milliseconds.
     */
    private          int         simulatedLatency;
    /**
     * Simulated network jitter, in milliseconds.
     */
    private          int         simulatedMaxJitter;
    /**
     * Simulated network bandwidth, in KB/sec.
     */
    private          int         simulatedBandwidth;
    private          InetAddress localAddress;
    private          int         localPort;
    private boolean allowSessionExpiry = true;

    private boolean memoryPreAllocation = false;

    /**
     * The consecutive number of EXP events before the session expires.
     */
    private int    expLimit                    = 16;
    /**
     * The initial congestion window size, in packets.
     */
    private double initialCongestionWindowSize = 16;
    /**
     * If larger than 0, the receiver should acknowledge every n'th packet.
     */
    private int    ackInterval                 = 16;
    /**
     * Microseconds to next EXP event. Default to 500 millis.
     */
    private long   expTimerInterval            = 50 * Util.getSYNTime();

    /**
     * Max ACK timer interval, in microseconds.
     */
    private long maxAckTimerInterval = 10 * Util.getSYNTime();

    /**
     * Whether to collect more detailed statistics or not.
     */
    private boolean deepStatistics = true;

    /**
     * Flow window size (how many data packets are in-flight at a single time).
     * <p>
     * Memory usage per session will be almost directly proportional to the window size,
     * as each window entry will require memory of one packet.
     * <p>
     * For high-throughput connections, thousands are recommended.
     */
//    private int flowWindowSize = 1024 * 10;
    private int flowWindowSize = 256;

    /**
     * Create a new instance.
     *
     * @param localAddress local address to bind to. null for default network interface of machine.
     * @param localPort    port to bind to. If 0, an ephemeral port is chosen.
     */
    public Config(final InetAddress localAddress, final int localPort) {
        this.localAddress = localAddress;
        this.localPort = localPort;
    }

    /**
     * Suitable for a latency insensitive, high-performance and high-throughput connection.
     * <p>
     * May not be scalable for high number of sessions as memory requirements
     * per session are considerable higher than the {@link #ofStandard(InetAddress, int) standard configuration}.
     *
     * @param localAddress the local address to bind on.
     * @param localPort    the local port to bind on.
     * @return the high throughput configuration.
     */
    public static Config ofHighThroughput(final InetAddress localAddress, final int localPort) {
        final Config config = new Config(localAddress, localPort);
        config.setFlowWindowSize(1024 * 10);
        config.setMemoryPreAllocation(true);
        return config;
    }

    /**
     * Standard configuration.
     * <p>
     * Good for low-latency scenarios and capable of handling many open sessions,
     * as memory requirements per session are low.
     *
     * @param localAddress the local address to bind on.
     * @param localPort    the local port to bind on.
     * @return the standard configuration.
     */
    public static Config ofStandard(final InetAddress localAddress, final int localPort) {
        final Config config = new Config(localAddress, localPort);
        config.setFlowWindowSize(256);
        config.setMemoryPreAllocation(false);
        return config;
    }

    public InetAddress getLocalAddress() {
        return localAddress;
    }

    public int getLocalPort() {
        return localPort;
    }

    public void setLocalPort(final int localPort) {
        this.localPort = localPort;
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
     * @param packetLossPercentage the packet loss as a percentage (ie. x, where 0 &lt;= x &lt;= 1.0).
     * @return this config.
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
     * @return the ACK interval. If larger than 0, the receiver should acknowledge
     * every n'th packet.
     */
    public int getAckInterval() {
        return ackInterval;
    }

    /**
     * Set the ACK interval. If larger than 0, the receiver should acknowledge
     * every n'th packet.
     *
     * @param ackInterval the ackInterval to set.
     */
    public void setAckInterval(final int ackInterval) {
        this.ackInterval = ackInterval;
    }

    public int getDatagramSize() {
        return datagramSize;
    }

    public int getExpLimit() {
        return expLimit;
    }

    public void setExpLimit(final int expLimit) {
        if (expLimit <= 1) throw new IllegalArgumentException("expLimit must be at least 2 or greater");
        this.expLimit = expLimit;
    }

    public long getExpTimerInterval() {
        return expTimerInterval;
    }

    public void setExpTimerInterval(long expTimerInterval) {
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

    public double getInitialCongestionWindowSize() {
        return initialCongestionWindowSize;
    }

    public void setInitialCongestionWindowSize(double initialCongestionWindowSize) {
        this.initialCongestionWindowSize = initialCongestionWindowSize;
    }

    public int getFlowWindowSize() {
        return flowWindowSize;
    }

    public void setFlowWindowSize(final int flowWindowSize) {
        this.flowWindowSize = flowWindowSize;
    }

    /**
     * @return whether memory should be pre-allocated for sessions.
     * @see Config#setMemoryPreAllocation(boolean)
     */
    public boolean isMemoryPreAllocation() {
        return memoryPreAllocation;
    }

    /**
     * Optimization strategy for memory vs. cpu.
     * <p>
     * If true, memory for all sessions will be pre-allocated. This will give
     * performance gains on very high-throughput connections, but leaves open
     * the vulnerability of OutOfMemoryErrors, especially on servers with many
     * open client sessions.
     *
     * @param memoryPreAllocation the value to set.
     */
    private void setMemoryPreAllocation(final boolean memoryPreAllocation) {
        this.memoryPreAllocation = memoryPreAllocation;
    }
}
