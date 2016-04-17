package io.lyracommunity.bolt.packet;

import io.lyracommunity.bolt.util.SeqNum;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * Negative Acknowledgement (NAK) carries information about lost packets.
 * <p>
 * Additional Info: Undefined
 * <p>
 * Control Info:
 * <ol>
 * <li> 32 bits integer array of compressed loss information (see section 3.9).
 * </ol>
 */
public class Nak extends ControlPacket {

    /**
     * After decoding this contains the lost sequence numbers.
     */
    private final List<Integer> lostSequenceNumbers;

    public Nak() {
        super(PacketType.NAK);
        lostSequenceNumbers = new ArrayList<>();
    }

    Nak(final byte[] controlInformation) {
        super(PacketType.NAK);
        lostSequenceNumbers = decode(controlInformation);
    }
//
//    public static void main(String[] args) {
//        final AtomicInteger result = new AtomicInteger(0);
//        final IntConsumer intAction = (a) -> {};
//        final Consumer<? super Integer> boxedIntAction = (a) -> {};
////        final Consumer<? super Integer> intAction = result::addAndGet;
//        final Nak nak = new Nak();
//        nak.addLossRange(600, 900);
//        nak.addLossRange(Arrays.asList(908, 988, 1655, 1788));
//        nak.addLossRange(SeqNum.MAX_SEQ_NUM_16_BIT - 2000, 500);
//        final int spins = 1_000_000;
//        Consumer<Runnable> timed = (r) -> {
//            final long start = System.nanoTime();
//            for (int i = 0; i < spins; i++) r.run();
//            System.out.println("TIME:  " + ((System.nanoTime() - start) / 1_000_000) + "ms");
//            System.out.println("RESULT:  " + result.get());
//        };
//        timed.accept( () -> nak.computeExpandedLossList().forEach(boxedIntAction));
//        result.set(0);
//        timed.accept( () -> nak.computeExpandedLossListLazy().forEach(intAction));
//    }

    public IntStream computeExpandedLossList() {
        IntStream result = IntStream.empty();
        final Iterator<Integer> lostSequence = getDecodedLossInfo().iterator();
        while (lostSequence.hasNext()) {
            final int read = lostSequence.next();
            final int lost = PacketUtil.setIntBit(read, 31, false);
            final boolean isSingle = !PacketUtil.isBitSet(read, 31);

            if (isSingle) {
                result = IntStream.concat(result, IntStream.of(lost));
            }
            else {
                // And add all lost numbers to the result list.
                final int end = lostSequence.next();

                if (end < lost) {
                    // There was a sequence number overflow.
                    result = IntStream.concat(result, IntStream.rangeClosed(lost, SeqNum.MAX_SEQ_NUM_16_BIT));
                    result = IntStream.concat(result, IntStream.rangeClosed(0, end));
                }
                else {
                    result = IntStream.concat(result, IntStream.rangeClosed(lost, end));
                }
            }
        }
        return result;
    }

    /**
     * Decode the loss info.
     *
     * @param lossInfo list of lost sequence numbers.
     */
    protected List<Integer> decode(final byte[] lossInfo) {
        final List<Integer> lostSequenceNumbers = new ArrayList<>();
        final ByteBuffer bb = ByteBuffer.wrap(lossInfo);
        while (bb.remaining() > 0) {
            final int lost = bb.getInt();
            lostSequenceNumbers.add(lost);
        }
        return lostSequenceNumbers;
    }

    /**
     * Add a single lost packet number.
     *
     * @param singleRelSeqNum packet sequence number that was lost.
     */
    private void addLossInfo(final int singleRelSeqNum) {
        final int enc = PacketUtil.setIntBit(singleRelSeqNum, 31, false);
        lostSequenceNumbers.add(enc);
    }

    /**
     * Add an interval of lost packet numbers.
     *
     * @param firstReliabilitySeqNumInclusive first number in range, inclusive.
     * @param lastReliabilitySeqNumInclusive  last number in rage, inclusive.
     */
    public void addLossRange(final int firstReliabilitySeqNumInclusive, final int lastReliabilitySeqNumInclusive) {
        // Check if we really need an interval
        if (lastReliabilitySeqNumInclusive - firstReliabilitySeqNumInclusive == 0) {
            addLossInfo(firstReliabilitySeqNumInclusive);
        }
        else {
            // Else add an interval
            lostSequenceNumbers.add(PacketUtil.setIntBit(firstReliabilitySeqNumInclusive, 31, true));
            lostSequenceNumbers.add(PacketUtil.setIntBit(lastReliabilitySeqNumInclusive, 31, false));
        }
    }

    /**
     * Pack the given list of sequence numbers and add them to the loss info.
     *
     * @param reliabilitySeqNums a list of sequence numbers.
     */
    public void addLossInfo(final List<Integer> reliabilitySeqNums) {
        int index = 0;
        do {
            int start = reliabilitySeqNums.get(index);
            int end = 0;
            int c = 0;
            do {
                c++;
                index++;
                if (index < reliabilitySeqNums.size()) {
                    end = reliabilitySeqNums.get(index);
                }
            }
            while (end - start == c);

            if (end == 0) {
                addLossInfo(start);
            }
            else {
                end = reliabilitySeqNums.get(index - 1);
                addLossRange(start, end);
            }
        }
        while (index < reliabilitySeqNums.size());
    }

    /**
     * @return the lost packet numbers
     */
    private List<Integer> getDecodedLossInfo() {
        return lostSequenceNumbers;
    }

    @Override
    public byte[] encodeControlInformation() {
        final ByteBuffer encoded = ByteBuffer.allocate(lostSequenceNumbers.size() * 4);
        lostSequenceNumbers.forEach(encoded::putInt);
        return encoded.array();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        final Nak nak = (Nak) o;
        return Objects.equals(lostSequenceNumbers, nak.lostSequenceNumbers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), lostSequenceNumbers);
    }

}
