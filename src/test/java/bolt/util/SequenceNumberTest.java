package bolt.util;

import org.junit.Test;

import java.util.Random;
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
    public void testOrderSeqNumOverflow() {
        final int begin = MAX_SEQ_NUM_16_BIT - 100;
        int end = IntStream.range(0, 200).reduce(begin, (acc, x) -> SequenceNumber.increment16(acc));
        assertEquals(99, end);
    }

    @Test
    public void testOrderSeqNumComparison() {
        assertEquals(-1, SequenceNumber.compare16(1, 2));
        assertEquals(-2, SequenceNumber.compare16(1, 3));
        assertEquals(1, SequenceNumber.compare16(2, 1));
        assertEquals(MAX_SEQ_NUM_16_BIT - 4, SequenceNumber.compare16(2, MAX_SEQ_NUM_16_BIT - 2));
    }

    @Test
    public void testVariableBitLengthOverflow() {
        final Random r = new Random();
        for (int i = 0; i < 10; i++) {
            final int max = 1 << (3 + r.nextInt(29));
            final int begin = max - 5;
            int end = IntStream.range(0, 10).reduce(begin, (acc, x) -> SequenceNumber.increment(acc, max));
            assertEquals(4, end);
        }
    }

    @Test
    public void testVariableBitLengthComparison() {
        final Random r = new Random();
        for (int i = 0; i < 100; i++) {
            final int max = (1 << (4 + r.nextInt(28))) - 1;
            final int maxOffset = max / 2;
            assertEquals(-1, SequenceNumber.compare(1, 2, maxOffset));
            assertEquals(-2, SequenceNumber.compare(1, 3, maxOffset));
            assertEquals(1, SequenceNumber.compare(2, 1, maxOffset));
            assertEquals(max - 4, SequenceNumber.compare(2, max - 2, maxOffset));
        }
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
