package bolt.util;

import bolt.packets.DataPacket;

import java.util.BitSet;

/**
 * Created by keen on 26/03/16.
 */
public class DuplicateDetector {

    private static final int MAX_DUP_BUFFER = 100_000;
    private static final int SEGMENTS = 8;
    private static final int ITEMS_PER_SEGMENT = MAX_DUP_BUFFER / SEGMENTS;
    private final BitSet received = new BitSet(MAX_DUP_BUFFER);

    private int lastDupNum = -1;

    public boolean checkDuplicatePacket(final DataPacket data) {
        final int dupNum = data.getPacketSeqNumber() % MAX_DUP_BUFFER;
        if (received.get(dupNum)) return true;
        received.set(dupNum, true);

        // Check segment reset
        if (lastDupNum >= 0) {
            final int lastSegmentSeqNum = lastDupNum / SEGMENTS;
            final int segmentSeqNum = dupNum / SEGMENTS;

            if (lastSegmentSeqNum != segmentSeqNum) {
                final int segmentToReset = (segmentSeqNum + 2) % SEGMENTS;
                final int start = segmentToReset * ITEMS_PER_SEGMENT;
                received.set(start, start + ITEMS_PER_SEGMENT);
            }

        }
        lastDupNum = dupNum;
        return false;
    }

}
