package io.lyracommunity.bolt.util;

import java.util.Random;


/**
 * Utility class for safely handling sequence numbers operations.
 * <p>
 * All operations account for sequence number wrap-around on overflow.
 *
 * @author Cian.
 */
public class SeqNum {


    public final static  int MAX_PACKET_SEQ_NUM = (int) (Math.pow(2, 28) - 1);
    public final static  int MAX_SEQ_NUM_16_BIT = (int) (Math.pow(2, 16) - 1);
    private final static int MAX_PACKET_OFFSET  = MAX_PACKET_SEQ_NUM / 2;
    private final static int MAX_OFFSET_16_BIT  = MAX_SEQ_NUM_16_BIT / 2;

    private final static Random RAND = new Random();

    /**
     * compare seq1 and seq2. Returns zero, if they are equal, a negative value if seq1 is smaller than
     * seq2, and a positive value if seq1 is larger than seq2.
     *
     * @param seq1 the first sequence number.
     * @param seq2 the second sequence number.
     */
    public static int comparePacketSeqNum(int seq1, int seq2) {
        return compare(seq1, seq2, MAX_PACKET_OFFSET);
    }

    /**
     * compare seq1 and seq2. Returns zero, if they are equal, a negative value if seq1 is smaller than
     * seq2, and a positive value if seq1 is larger than seq2.
     *
     * @param seq1 the first sequence number.
     * @param seq2 the second sequence number.
     */
    public static int compare16(int seq1, int seq2) {
        return compare(seq1, seq2, MAX_OFFSET_16_BIT);
    }

    /**
     * compare seq1 and seq2. Returns zero, if they are equal, a negative value if seq1 is smaller than
     * seq2, and a positive value if seq1 is larger than seq2.
     *
     * @param seq1 the first sequence number.
     * @param seq2 the second sequence number.
     */
    static int compare(int seq1, int seq2, int maxOffset) {
        return (Math.abs(seq1 - seq2) < maxOffset) ? (seq1 - seq2) : (seq2 - seq1);
    }

    /**
     * Length from the first to the second sequence number, including both.
     *
     * @param seq1 the first sequence number.
     * @param seq2 the second sequence number.
     * @return the computed length.
     */
    public static int length(int seq1, int seq2) {
        return (seq1 <= seq2) ? (seq2 - seq1 + 1) : (seq2 - seq1 + MAX_PACKET_SEQ_NUM + 2);
    }

    /**
     * Compute the offset from seq2 to seq1.
     *
     * @param seq1 the first sequence number.
     * @param seq2 the second sequence number.
     * @return the sequence number offset.
     */
    public static int seqOffsetPacketSeqNum(int seq1, int seq2) {
        return seqOffset(seq1, seq2, MAX_PACKET_OFFSET, MAX_PACKET_SEQ_NUM);
    }

    public static int seqOffset16(int seq1, int seq2) {
        return seqOffset(seq1, seq2, MAX_OFFSET_16_BIT, MAX_SEQ_NUM_16_BIT);
    }

    private static int seqOffset(int seq1, int seq2, int maxOffset, int maxSeqNum) {
        if (Math.abs(seq1 - seq2) < maxOffset)
            return seq2 - seq1;

        if (seq1 < seq2)
            return seq2 - seq1 - maxSeqNum - 1;

        return seq2 - seq1 + maxSeqNum + 1;
    }

    /**
     * Increment a 28-bit sequence number.
     *
     * @param seq the sequence number to increment.
     * @return the incremented sequence number.
     */
    public static int incrementPacketSeqNum(int seq) {
        return (seq == MAX_PACKET_SEQ_NUM) ? 0 : seq + 1;
    }

    /**
     * Increment a 16-bit sequence number.
     *
     * @param seq the sequence number to increment.
     * @return the incremented sequence number.
     */
    public static int increment16(int seq) {
        return (seq == MAX_SEQ_NUM_16_BIT) ? 0 : seq + 1;
    }

    /**
     * Increment by one.
     *
     * @return the incremented sequence number.
     */
    public static int increment(int seq, int max) {
        return (seq == max) ? 0 : seq + 1;
    }

    /**
     * Add to a sequence number.
     *
     * @param seq the sequence number to add to.
     * @param add the amount to add.
     * @param max the max seq number before wrap-around.
     * @return the incremented sequence number.
     */
    public static int add(int seq, int add, int max) {
        final int added = seq + add;
        return (added >= max) ? added % max : added;
    }


    /**
     * Add to a 16-bit sequence number.
     *
     * @return the added sequence number.
     */
    public static int add16(int seq, int add) {
        return add(seq, add, MAX_SEQ_NUM_16_BIT);
    }

    /**
     * Decrement by one.
     *
     * @param seq number to decrement.
     * @return the new sequence number.
     */
    public static int decrement(int seq) {
        return (seq == 0) ? MAX_PACKET_SEQ_NUM : seq - 1;
    }

    /**
     * Generates a random packet sequence number.
     *
     * @return the generated sequence number.
     */
    public static int randomPacketSeqNum() {
        return 1 + RAND.nextInt(MAX_PACKET_OFFSET);
    }

    public static int randomInt() {
        return RAND.nextInt();
    }

}
