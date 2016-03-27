package bolt.util;

import java.util.Random;


/**
 * Handle sequence numbers, taking the range of 0 - (2^31 - 1) into account<br/>
 */

public class SeqNum {


    public final static int MAX_PACKET_SEQ_NUM = (int) (Math.pow(2, 28) - 1);
    private final static int MAX_PACKET_OFFSET = MAX_PACKET_SEQ_NUM / 2;

    public final static int MAX_SEQ_NUM_16_BIT = (int) (Math.pow(2, 16) - 1);
    private final static int MAX_OFFSET_16_BIT = MAX_SEQ_NUM_16_BIT / 2;

    private final static Random rand = new Random();

    /**
     * compare seq1 and seq2. Returns zero, if they are equal, a negative value if seq1 is smaller than
     * seq2, and a positive value if seq1 is larger than seq2.
     *
     * @param seq1
     * @param seq2
     */
    public static int comparePacketSeqNum(int seq1, int seq2) {
        return compare(seq1, seq2, MAX_PACKET_OFFSET);
    }

    /**
     * compare seq1 and seq2. Returns zero, if they are equal, a negative value if seq1 is smaller than
     * seq2, and a positive value if seq1 is larger than seq2.
     *
     * @param seq1
     * @param seq2
     */
    public static int compare16(int seq1, int seq2) {
        return compare(seq1, seq2, MAX_OFFSET_16_BIT);
    }

    /**
     * compare seq1 and seq2. Returns zero, if they are equal, a negative value if seq1 is smaller than
     * seq2, and a positive value if seq1 is larger than seq2.
     *
     * @param seq1
     * @param seq2
     */
    static int compare(int seq1, int seq2, int maxOffset) {
        return (Math.abs(seq1 - seq2) < maxOffset) ? (seq1 - seq2) : (seq2 - seq1);
    }

    /**
     * length from the first to the second sequence number, including both
     */
    public static int length(int seq1, int seq2) {
        return (seq1 <= seq2) ? (seq2 - seq1 + 1) : (seq2 - seq1 + MAX_PACKET_SEQ_NUM + 2);
    }

    /**
     * compute the offset from seq2 to seq1
     *
     * @param seq1
     * @param seq2
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
     * increment by one
     *
     * @param seq
     */
    public static int incrementPacketSeqNum(int seq) {
        return (seq == MAX_PACKET_SEQ_NUM) ? 0 : seq + 1;
    }

    /**
     * increment by one
     *
     * @param seq
     */
    public static int increment16(int seq) {
        return (seq == MAX_SEQ_NUM_16_BIT) ? 0 : seq + 1;
    }

    /**
     * increment by one
     *
     * @param seq
     */
    public static int increment(int seq, int max) {
        return (seq == max) ? 0 : seq + 1;
    }

    /**
     * decrement by one
     *
     * @param seq
     */
    public static int decrement(int seq) {
        return (seq == 0) ? MAX_PACKET_SEQ_NUM : seq - 1;
    }

    /**
     * Generates a random number between 1 and {@value MAX_PACKET_OFFSET} (inclusive).
     */
    public static int randomPacketSeqNum() {
        return 1 + rand.nextInt(MAX_PACKET_OFFSET);
    }

    public static int randomInt() {
        return rand.nextInt();
    }

}
