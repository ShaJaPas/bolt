package bolt.packet;

import bolt.BoltSender;

/**
 * Acknowledgement of Acknowledgement (ACK2) is sent by the {@link BoltSender}
 * as immediate reply to an {@link Acknowledgement}.
 * <p>
 * Additional Info: ACK sequence number     <br>
 * Control Info: None                       <br>
 */
public class Acknowledgment2 extends ControlPacket {


    /** The ACK sequence number */
    private long ackSequenceNumber;

    public Acknowledgment2() {
        this.controlPacketType = ControlPacketType.ACK2.getTypeId();
    }

    public Acknowledgment2(long ackSeqNo, byte[] controlInformation) {
        this();
        this.ackSequenceNumber = ackSeqNo;
        decode(controlInformation);
    }

    public long getAckSequenceNumber() {
        return ackSequenceNumber;
    }

    public void setAckSequenceNumber(long ackSequenceNumber) {
        this.ackSequenceNumber = ackSequenceNumber;
    }

    void decode(byte[] data) {
        ackSequenceNumber = PacketUtil.decode(data, 0);
    }

    public boolean forSender() {
        return false;
    }

    @Override
    public byte[] encodeControlInformation() {
        return PacketUtil.encode(ackSequenceNumber);
    }
}



