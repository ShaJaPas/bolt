package io.lyracommunity.bolt.packet;

import io.lyracommunity.bolt.sender.BoltSender;

/**
 * Acknowledgement of Acknowledgement (ACK2) is sent by the {@link BoltSender}
 * as immediate reply to an {@link Ack}.
 * <p>
 * Additional Info: ACK sequence number
 * <p>
 * Control Info: None
 */
public class Ack2 extends ControlPacket {


    /** The ACK sequence number */
    private long ackSequenceNumber;

    public static Ack2 build(final long ackSequenceNumber, final int destinationID) {
        return new Ack2(ackSequenceNumber, destinationID);
    }

    public Ack2() {
        this.controlPacketType = PacketType.ACK2.getTypeId();
    }

    Ack2(long ackSeqNo, byte[] controlInformation) {
        this();
        this.ackSequenceNumber = ackSeqNo;
        decode(controlInformation);
    }

    Ack2(final long ackSequenceNumber, final int destinationID)
    {
        this();
        this.ackSequenceNumber = ackSequenceNumber;
        this.destinationID = destinationID;
    }

    public long getAckSequenceNumber() {
        return ackSequenceNumber;
    }

    void decode(final byte[] data) {
        ackSequenceNumber = PacketUtil.decode(data, 0);
    }

    @Override
    public byte[] encodeControlInformation() {
        return PacketUtil.encode(ackSequenceNumber);
    }
}



