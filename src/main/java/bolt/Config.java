package bolt;

import java.net.InetAddress;

/**
 * Created by omahoc9 on 3/11/16.
 */
public class Config
{

    private volatile float packetDropRate;

    private InetAddress localAddress;

    private int localPort;

//    private final int datagramSize = 1400;

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


}
