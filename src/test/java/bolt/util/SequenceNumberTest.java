package bolt.util;

import org.junit.Test;

import java.util.stream.IntStream;

import static bolt.util.SequenceNumber.MAX_PACKET_SEQ_NUM;
import static bolt.util.SequenceNumber.MAX_SEQ_NUM_16_BIT;
import static org.junit.Assert.assertEquals;

/**
 * Created by keen on 26/03/16.
 */
public class SequenceNumberTest {

    @Test
    public void testPacketSequenceNumberOverflow() {
        final int begin = MAX_PACKET_SEQ_NUM - 100;
        int end = IntStream.range(0, 200).reduce(begin, (acc, x) -> SequenceNumber.incrementPacketSeqNum(acc));
        assertEquals(99, end);
    }

    @Test
    public void testPacketSequenceNumberComparison() {
        assertEquals(-1, SequenceNumber.comparePacketSeqNum(1, 2));
        assertEquals(-2, SequenceNumber.comparePacketSeqNum(1, 3));
        assertEquals(1, SequenceNumber.comparePacketSeqNum(2, 1));
        assertEquals(MAX_PACKET_SEQ_NUM - 4, SequenceNumber.comparePacketSeqNum(2, MAX_PACKET_SEQ_NUM - 2));
    }

    @Test
    public void testVariableBitLengthOverflow() {
    }

    @Test
    public void testVariableBitLengthComparison() {

    }

    @Test
    public void testOrderSequenceNumberOffset_overflow() {
        assertEquals(1, SequenceNumber.seqOffset16(MAX_SEQ_NUM_16_BIT, 0));
    }

    @Test
    public void testOrderSequenceNumberOffset() {
        assertEquals(-1, SequenceNumber.seqOffset16(MAX_SEQ_NUM_16_BIT, MAX_SEQ_NUM_16_BIT - 1));
    }


}
