package bolt.receiver;

import bolt.util.CircularArray;

/**
 * Packet Pair Window is a circular array that records the time
 * interval between each probing packet pair.
 *
 * @see {@link CircularArray}
 */
public class PacketPairWindow extends CircularArray<Long> {

    /**
     * construct a new packet pair window with the given size
     *
     * @param size
     */
    public PacketPairWindow(int size) {
        super(size);
    }

    /**
     * compute the median packet pair interval of the last
     * 16 packet pair intervals (PI).
     * (see specification section 6.2, page 12)
     *
     * @return time interval in microseconds
     */
    public double computeMedianTimeInterval() {
        int num = size();
        double median = 0;
        for (int i = 0; i < num; i++) {
            median += getEntry(i).doubleValue();
        }
        median = median / num;

        //median filtering
        double upper = median * 8;
        double lower = median / 8;
        double total = 0;
        int count = 0;
        for (int i = 0; i < num; i++) {
            double val = getEntry(i).doubleValue();
            if (val < upper && val > lower) {
                total += val;
                count++;
            }
        }
        double res = total / count;
        return res;
    }

    /**
     * compute the estimated linK capacity using the values in
     * packet pair window
     *
     * @return number of packets per second
     */
    public long getEstimatedLinkCapacity() {
        long res = (long) Math.ceil(1000000 / computeMedianTimeInterval());
        return res;
    }
}
