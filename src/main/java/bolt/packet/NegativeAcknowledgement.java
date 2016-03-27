package bolt.packet;

import bolt.util.SeqNum;

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
public class NegativeAcknowledgement extends ControlPacket {

    // After decoding this contains the lost sequence numbers
    List<Integer> lostSequenceNumbers;

    // This contains the loss information intervals as described on p.15 of the spec
    ByteArrayOutputStream lossInfo = new ByteArrayOutputStream();

    public NegativeAcknowledgement() {
        this.controlPacketType = ControlPacketType.NAK.getTypeId();
    }

    public NegativeAcknowledgement(byte[] controlInformation) {
        this();
        lostSequenceNumbers = decode(controlInformation);
    }

    /**
     * Decode the loss info.
     *
     * @param lossInfo list of lost sequence numbers.
     */
    private List<Integer> decode(byte[] lossInfo) {
        List<Integer> lostSequenceNumbers = new ArrayList<>();
        ByteBuffer bb = ByteBuffer.wrap(lossInfo);
        byte[] buffer = new byte[4];
        while (bb.remaining() > 0) {
            // Read 4 bytes
            buffer[0] = bb.get();
            buffer[1] = bb.get();
            buffer[2] = bb.get();
            buffer[3] = bb.get();
            boolean isNotSingle = (buffer[0] & 128) != 0;
            // Set highest bit back to 0
            buffer[0] = (byte) (buffer[0] & 0x7f);
            int lost = ByteBuffer.wrap(buffer).getInt();
            if (isNotSingle) {
                // Get the end of the interval
                int end = bb.getInt();
                // And add all lost numbers to the result list
                // TODO what about overflow? lost = 65536, end = 0
                for (int i = lost; SeqNum.compare16(i, end) <= 0; i = SeqNum.increment16(i)) {
//                for (int i = lost; i <= end; i++) {
                    lostSequenceNumbers.add(i);
                }
            } else {
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
    public void addLossInfo(long singleSequenceNumber) {
        byte[] enc = PacketUtil.encodeSetHighest(false, singleSequenceNumber);
        try {
            lossInfo.write(enc);
        } catch (IOException ignore) {
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
        } catch (IOException ignore) {
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
            } else {
                end = sequenceNumbers.get(index - 1);
                addLossInfo(start, end);
            }
        }
        while (index < sequenceNumbers.size());
    }

    /**
     * Return the lost packet numbers
     *
     * @return
     */
    public List<Integer> getDecodedLossInfo() {
        return lostSequenceNumbers;
    }

    @Override
    public byte[] encodeControlInformation() {
        try {
            if (lossInfo.size() == 0) {
                System.out.println("BLAH");
            }
            return lossInfo.toByteArray();
        } catch (Exception e) {
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
        NegativeAcknowledgement other = (NegativeAcknowledgement) obj;

        final List<Integer> thisLost;
        final List<Integer> otherLost;

        // Compare the loss info
        if (lostSequenceNumbers != null) {
            thisLost = lostSequenceNumbers;
        } else {
            thisLost = decode(lossInfo.toByteArray());
        }
        if (other.lostSequenceNumbers != null) {
            otherLost = other.lostSequenceNumbers;
        } else {
            otherLost = other.decode(other.lossInfo.toByteArray());
        }
        if (!thisLost.equals(otherLost)) {
            return false;
        }

        return true;
    }


}
