package bolt;

/**
 * Created by omahoc9 on 3/11/16.
 */
public class Config
{

    private volatile float packetDropRate;

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
    public void setPacketLoss(final float packetLossPercentage) {
        final float normalizedPacketLossPercentage = Math.min(packetLossPercentage, 1f);
        packetDropRate = (normalizedPacketLossPercentage <= 0f) ? 0f : 1f / normalizedPacketLossPercentage;
    }


}
