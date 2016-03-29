package io.lyracommunity.bolt.receiver;

import io.lyracommunity.bolt.util.CircularArray;

/**
 * Packet History Window is a circular array that records the arrival time
 * of each data packet.
 */
public class PacketHistoryWindow extends CircularArray<Long> {

    private final long[] intervals;
    private final int num;

    /**
     * Create a new PacketHistoryWindow of the given size.
     *
     * @param size the size of the window.
     */
    public PacketHistoryWindow(final int size) {
        super(size);
        num = max - 1;
        intervals = new long[num];
    }

    /**
     * Compute the packet arrival speed.
     *
     * @return the current value.
     */
    public long getPacketArrivalSpeed() {
        if (!haveOverflow) return 0;

        double total = 0;
        int count = 0;
        int pos = (position - 1 < 0) ? num : (position - 1);
        do {
            final long upper = getEntry(pos);
            pos--;
            if (pos < 0) pos = num;
            final long lower = getEntry(pos);
            final long interval = upper - lower;
            intervals[count] = interval;
            total += interval;
            count++;
        }
        while (count < num);
        // Compute median
        final double AI = total / num;
        // Compute the actual value, filtering out intervals between AI/8 and AI*8.
        count = 0;
        total = 0;
        for (final long l : intervals) {
            if (l > AI / 8d && l < AI * 8d) {
                total += l;
                count++;
            }
        }

        final double medianPacketArrivalSpeed = (count > 8)
                ? 1e6 * count / total
                : 0;

        return (long) Math.ceil(medianPacketArrivalSpeed);
    }

}
