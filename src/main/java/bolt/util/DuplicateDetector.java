package bolt.util;

import bolt.packets.DataPacket;

import java.util.BitSet;

/**
 * Prevent replaying of previously-received packets.
 * <p>
 * A bounded circular buffer holds the packet sequence numbers
 * of previously received packets. This will guarantee at-most-once
 * delivery within a limit. This limit is determined by the specified
 * size of the bounded buffer. The larger the buffer, the less chance
 * of a duplicate packet being missed.
 *
 * @author Cian O'Mahony
 */
public class DuplicateDetector {

    static final int DEFAULT_SEGMENT_COUNT = 8;

    private final int segments;
    private final int itemsPerSegment;
    private final BitSet received;

    private int lastDupNum = -1;

    private DuplicateDetector(int segments, int itemsPerSegment, BitSet received) {
        this.segments = segments;
        this.itemsPerSegment = itemsPerSegment;
        this.received = received;
    }

    public static DuplicateDetector ofSize(final int size) {
        final BitSet set = new BitSet(size);
        return fromBitSet(set);
    }

    public static DuplicateDetector fromBitSet(final BitSet set) {
        return new DuplicateDetector(DEFAULT_SEGMENT_COUNT, set.size() / DEFAULT_SEGMENT_COUNT, set);
    }

    public boolean checkDuplicatePacket(final DataPacket data) {
        final int duplicationId = getDuplicationId(data);

        final boolean isDuplicate = received.get(duplicationId);

        if (!isDuplicate) {

            // Mark id as received.
            received.set(duplicationId, true);

            // Check segment reset
            checkSegmentReset(duplicationId);

            lastDupNum = duplicationId;
        }
        return isDuplicate;
    }

    private void checkSegmentReset(final int currentDuplicationId) {
        if (lastDupNum >= 0) {
            final int lastSegmentSeqNum = lastDupNum / itemsPerSegment;
            final int segmentSeqNum = currentDuplicationId / itemsPerSegment;

            if (lastSegmentSeqNum != segmentSeqNum) {
                final int segmentToReset = (segmentSeqNum + (segments / 2)) % segments;
                final int start = segmentToReset * itemsPerSegment;
                received.set(start, start + itemsPerSegment, false);
            }
        }
    }

    private int getDuplicationId(DataPacket data) {
        return data.getPacketSeqNumber() % received.size();
    }

}
