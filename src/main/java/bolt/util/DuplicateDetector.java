package bolt.util;

import bolt.packets.DataPacket;

import java.util.BitSet;

/**
 * Created by keen on 26/03/16.
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
        if (received.get(duplicationId)) {
            return true;
        }
        received.set(duplicationId, true);

        // Check segment reset
        if (lastDupNum >= 0) {
            final int lastSegmentSeqNum = lastDupNum / itemsPerSegment;
            final int segmentSeqNum = duplicationId / itemsPerSegment;

            if (lastSegmentSeqNum != segmentSeqNum) {
                final int segmentToReset = (segmentSeqNum + (segments / 2)) % segments;
                final int start = segmentToReset * itemsPerSegment;
                received.set(start, start + itemsPerSegment, false);
            }

        }
        lastDupNum = duplicationId;
        return false;
    }

    private int getDuplicationId(DataPacket data) {
        return data.getPacketSeqNumber() % received.size();
    }

}
