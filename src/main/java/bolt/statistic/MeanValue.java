package bolt.statistic;

import bolt.util.Util;

import java.text.NumberFormat;
import java.util.Locale;

/**
 * Holds a floating mean timing value (measured in microseconds).
 */
public class MeanValue {

    private final NumberFormat format;
    private final String name;
    private double mean = 0;
    private double max = 0;
    private double min = 0;
    private int n = 0;
    private long start;


    public MeanValue(final String name) {
        format = NumberFormat.getNumberInstance(Locale.ENGLISH);
        format.setMaximumFractionDigits(2);
        format.setGroupingUsed(false);
        this.name = name;
    }

    public void addValue(final double value) {
        mean = (mean * n + value) / (n + 1);
        n++;
        max = Math.max(max, value);
        min = Math.min(max, value);
    }

    public double getMean() {
        return mean;
    }

    public String getFormattedMean() {
        return format.format(getMean());
    }

    public String get() {
        return format.format(getMean()) + " max=" + format.format(max) + " min=" + format.format(min);
    }

    public void clear() {
        mean = 0;
        n = 0;
    }

    public void begin() {
        start = Util.getCurrentTime();
    }

    public void end() {
        if (start > 0) addValue(Util.getCurrentTime() - start);
    }

    public String getName() {
        return name;
    }

    public String toString() {
        return name;
    }
}
