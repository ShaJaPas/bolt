package io.lyracommunity.bolt.statistic;


/**
 * Holds a floating mean value.
 */
public class MeanThroughput extends MeanValue {

    private final double packetSize;

    public MeanThroughput(String name, int packetSize) {
        super(name);
        this.packetSize = packetSize;
    }

    @Override
    public double getMean() {
        return packetSize / super.getMean();
    }

    @Override
    String getFormattedMean() {
        return format.format(getMean());
    }

}
