package io.lyracommunity.bolt.receiver;

import io.lyracommunity.bolt.util.CircularArray;

/**
 * Packet Pair Window is a circular array that records the time
 * interval between each probing packet pair.
 *
 * @author Cian.
 * @see CircularArray
 */
class PacketPairWindow extends CircularArray<Long> {

    /**
     * Construct a new packet pair window with the given size
     *
     * @param size the size of the window.
     */
    PacketPairWindow(int size) {
        super(size);
    }

    /**
     * Compute the median packet pair interval of the last 16 packet pair intervals (PI).
     *
     * @return time interval in microseconds.
     */
    double computeMedianTimeInterval() {
        final int num = size();
        double median = 0;
        for (int i = 0; i < num; i++) {
            median += getEntry(i).doubleValue();
        }
        median = median / num;

        // Median filtering
        final double upper = median * 8;
        final double lower = median / 8;
        double total = 0;
        int count = 0;
        for (int i = 0; i < num; i++) {
            double val = getEntry(i).doubleValue();
            if (val < upper && val > lower) {
                total += val;
                count++;
            }
        }
        return total / count;
    }

    /**
     * Compute the estimated link capacity using the values in packet pair window.
     *
     * @return number of packets per second.
     */
    long getEstimatedLinkCapacity() {
        return (long) Math.ceil(1_000_000 / computeMedianTimeInterval());
    }

}
