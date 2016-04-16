package io.lyracommunity.bolt.packet;

import io.lyracommunity.bolt.util.SeqNum;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

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

//    /**
//     * This contains the loss information intervals.
//     */
//    final private ByteArrayOutputStream lossInfo = new ByteArrayOutputStream();
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

    public List<Integer> computeExpandedLossList() {
        final List<Integer> expanded = new ArrayList<>();
        final Iterator<Integer> lostSequence = getDecodedLossInfo().iterator();
        while (lostSequence.hasNext()) {
            final int read = lostSequence.next();
            final int lost = PacketUtil.setIntBit(read, 31, false);
            final boolean isNotSingle = PacketUtil.isBitSet(read, 31);

            // Add the first.
            expanded.add(lost);

            // Add the tail of the sequence, if any.
            if (isNotSingle) {
                final int end = lostSequence.next();
                // And add all lost numbers to the result list.
                for (int i = lost + 1; SeqNum.compare16(i, end) <= 0; i = SeqNum.increment16(i)) {
                    expanded.add(i);
                }
            }
        }
        return expanded;
    }

    /**
     * Decode the loss info.
     *
     * @param lossInfo list of lost sequence numbers.
     */
    protected List<Integer> decode(final byte[] lossInfo) {
        final List<Integer> lostSequenceNumbers = new ArrayList<>();
        final ByteBuffer bb = ByteBuffer.wrap(lossInfo);
//        byte[] buffer = new byte[4];
        while (bb.remaining() > 0) {
            final int lost = bb.getInt();
            lostSequenceNumbers.add(lost);
//            // Read 4 bytes
//            buffer[0] = bb.get();
//            buffer[1] = bb.get();
//            buffer[2] = bb.get();
//            buffer[3] = bb.get();
//            final boolean isNotSingle = (buffer[0] & 128) != 0;
//            // Set highest bit back to 0
//            buffer[0] = (byte) (buffer[0] & 0x7f);
//            final int lost = ByteBuffer.wrap(buffer).getInt();
//            if (isNotSingle) {
//                // Get the end of the interval
//                int end = bb.getInt();
//                // And add all lost numbers to the result list
//                for (int i = lost; SeqNum.compare16(i, end) <= 0; i = SeqNum.increment16(i)) {
//                    lostSequenceNumbers.add(i);
//                }
//            }
//            else {
//                lostSequenceNumbers.add(lost);
//            }
        }
        return lostSequenceNumbers;
    }

    /**
     * Add a single lost packet number.
     *
     * @param singleRelSeqNum packet sequence number that was lost.
     */
    private void addLossInfo(final int singleRelSeqNum) {
//        byte[] enc = PacketUtil.encodeSetHighest(false, singleRelSeqNum);
//        try {
            final int enc = PacketUtil.setIntBit(singleRelSeqNum, 31, false);
            lostSequenceNumbers.add(enc);
//            lossInfo.write(enc);
//        }
//        catch (IOException ignore) {
//        }
    }

    /**
     * Add an interval of lost packet numbers.
     *
     * @param firstSequenceNumber
     * @param lastSequenceNumber
     */
    public void addLossInfo(final int firstSequenceNumber, final int lastSequenceNumber) {
        // Check if we really need an interval
        if (lastSequenceNumber - firstSequenceNumber == 0) {
            addLossInfo(firstSequenceNumber);
        }
        else {
            // Else add an interval
            lostSequenceNumbers.add(PacketUtil.setIntBit(firstSequenceNumber, 31, true));
            lostSequenceNumbers.add(PacketUtil.setIntBit(lastSequenceNumber, 31, false));
            //        byte[] enc1 = PacketUtil.encodeSetHighest(true, firstSequenceNumber);
            //        byte[] enc2 = PacketUtil.encodeSetHighest(false, lastSequenceNumber);
            //        try {
            //            lossInfo.write(enc1);
            //            lossInfo.write(enc2);
            //        }
            //        catch (IOException ignore) {
            //        }
        }
    }

    /**
     * pack the given list of sequence numbers and add them to the loss info
     *
     * @param sequenceNumbers a list of sequence numbers.
     */
    public void addLossInfo(final List<Integer> sequenceNumbers) {
        int index = 0;
        do {
            int start = sequenceNumbers.get(index);
            int end = 0;
            int c = 0;
            do {
                c++;
                index++;
                if (index < sequenceNumbers.size()) {
                    end = sequenceNumbers.get(index);
                }
            }
            while (end - start == c);

            if (end == 0) {
                addLossInfo(start);
            }
            else {
                end = sequenceNumbers.get(index - 1);
                addLossInfo(start, end);
            }
        }
        while (index < sequenceNumbers.size());
    }

    /**
     * @return the lost packet numbers
     */
    private List<Integer> getDecodedLossInfo() {
        return lostSequenceNumbers;
    }

    @Override
    public byte[] encodeControlInformation() {
        try {
            final ByteBuffer encoded = ByteBuffer.allocate(lostSequenceNumbers.size() * 4);
            lostSequenceNumbers.forEach(encoded::putInt);
            return encoded.array();
        }
        catch (Exception e) {
            // can't happen
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        Nak nak = (Nak) o;
        return Objects.equals(lostSequenceNumbers, nak.lostSequenceNumbers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), lostSequenceNumbers);
    }

}
