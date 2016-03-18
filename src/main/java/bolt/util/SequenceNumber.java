package bolt.util;

import java.util.Random;


/**
 * Handle sequence numbers, taking the range of 0 - (2^31 - 1) into account<br/>
 */

public class SequenceNumber {


    public final static int MAX_SEQ_NUM = (int) (Math.pow(2, 28) - 1);
    private final static int MAX_OFFSET = MAX_SEQ_NUM / 2;

    private final static Random rand = new Random();

    /**
     * compare seq1 and seq2. Returns zero, if they are equal, a negative value if seq1 is smaller than
     * seq2, and a positive value if seq1 is larger than seq2.
     *
     * @param seq1
     * @param seq2
     */
    public static int compare(int seq1, int seq2) {
        return (Math.abs(seq1 - seq2) < MAX_OFFSET) ? (seq1 - seq2) : (seq2 - seq1);
    }

    /**
     * length from the first to the second sequence number, including both
     */
    public static int length(int seq1, int seq2) {
        return (seq1 <= seq2) ? (seq2 - seq1 + 1) : (seq2 - seq1 + MAX_SEQ_NUM + 2);
    }

    /**
     * compute the offset from seq2 to seq1
     *
     * @param seq1
     * @param seq2
     */
    public static int seqOffset(int seq1, int seq2) {
        if (Math.abs(seq1 - seq2) < MAX_OFFSET)
            return seq2 - seq1;

        if (seq1 < seq2)
            return seq2 - seq1 - MAX_SEQ_NUM - 1;

        return seq2 - seq1 + MAX_SEQ_NUM + 1;
    }

    /**
     * increment by one
     *
     * @param seq
     */
    public static int increment(int seq) {
        return (seq == MAX_SEQ_NUM) ? 0 : seq + 1;
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
        return (seq == 0) ? MAX_SEQ_NUM : seq - 1;
    }

    /**
     * Generates a random number between 1 and {@value MAX_OFFSET} (inclusive).
     */
    public static int random() {
        return 1 + rand.nextInt(MAX_OFFSET);
    }

}
