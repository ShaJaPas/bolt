package io.lyracommunity.bolt.packet;

import io.lyracommunity.bolt.util.SeqNum;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
     * This contains the loss information intervals.
     */
    final private ByteArrayOutputStream lossInfo = new ByteArrayOutputStream();
    /**
     * After decoding this contains the lost sequence numbers.
     */
    private List<Integer> lostSequenceNumbers;

    public Nak() {
        super(PacketType.NAK);
    }

    Nak(final byte[] controlInformation) {
        this();
        lostSequenceNumbers = decode(controlInformation);
    }

    /**
     * Decode the loss info.
     *
     * @param lossInfo list of lost sequence numbers.
     */
    protected List<Integer> decode(final byte[] lossInfo) {
        final List<Integer> lostSequenceNumbers = new ArrayList<>();
        final ByteBuffer bb = ByteBuffer.wrap(lossInfo);
        byte[] buffer = new byte[4];
        while (bb.remaining() > 0) {
            // Read 4 bytes
            buffer[0] = bb.get();
            buffer[1] = bb.get();
            buffer[2] = bb.get();
            buffer[3] = bb.get();
            final boolean isNotSingle = (buffer[0] & 128) != 0;
            // Set highest bit back to 0
            buffer[0] = (byte) (buffer[0] & 0x7f);
            final int lost = ByteBuffer.wrap(buffer).getInt();
            if (isNotSingle) {
                // Get the end of the interval
                int end = bb.getInt();
                // And add all lost numbers to the result list
                for (int i = lost; SeqNum.compare16(i, end) <= 0; i = SeqNum.increment16(i)) {
                    lostSequenceNumbers.add(i);
                }
            }
            else {
                lostSequenceNumbers.add(lost);
            }
        }
        return lostSequenceNumbers;
    }

    /**
     * Add a single lost packet number.
     *
     * @param singleSequenceNumber packet sequence number that was lost.
     */
    void addLossInfo(long singleSequenceNumber) {
        byte[] enc = PacketUtil.encodeSetHighest(false, singleSequenceNumber);
        try {
            lossInfo.write(enc);
        }
        catch (IOException ignore) {
        }
    }

    /**
     * Add an interval of lost packet numbers.
     *
     * @param firstSequenceNumber
     * @param lastSequenceNumber
     */
    public void addLossInfo(long firstSequenceNumber, long lastSequenceNumber) {
        // Check if we really need an interval
        if (lastSequenceNumber - firstSequenceNumber == 0) {
            addLossInfo(firstSequenceNumber);
            return;
        }
        // Else add an interval
        byte[] enc1 = PacketUtil.encodeSetHighest(true, firstSequenceNumber);
        byte[] enc2 = PacketUtil.encodeSetHighest(false, lastSequenceNumber);
        try {
            lossInfo.write(enc1);
            lossInfo.write(enc2);
        }
        catch (IOException ignore) {
        }
    }

    /**
     * pack the given list of sequence numbers and add them to the loss info
     *
     * @param sequenceNumbers - a list of sequence numbers
     */
    public void addLossInfo(List<Integer> sequenceNumbers) {
        int index = 0;
        do {
            long start = sequenceNumbers.get(index);
            long end = 0;
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
    public List<Integer> getDecodedLossInfo() {
        return lostSequenceNumbers;
    }

    @Override
    public byte[] encodeControlInformation() {
        try {
            return lossInfo.toByteArray();
        }
        catch (Exception e) {
            // can't happen
            return null;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        Nak other = (Nak) obj;

        final List<Integer> thisLost;
        final List<Integer> otherLost;

        // Compare the loss info
        if (lostSequenceNumbers != null) {
            thisLost = lostSequenceNumbers;
        }
        else {
            thisLost = decode(lossInfo.toByteArray());
        }
        if (other.lostSequenceNumbers != null) {
            otherLost = other.lostSequenceNumbers;
        }
        else {
            otherLost = other.decode(other.lossInfo.toByteArray());
        }
        if (!thisLost.equals(otherLost)) {
            return false;
        }

        return true;
    }


}
