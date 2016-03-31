package io.lyracommunity.bolt.packet;


import io.lyracommunity.bolt.sender.BoltSender;
import io.lyracommunity.bolt.util.SeqNum;

public interface BoltPacket extends Comparable<BoltPacket> {

    /**
     * Get the ID of the destination socket.
     */
    int getDestinationID();

    /**
     * Identifies whether this is a control packet (more performant than instanceof).
     */
    boolean isControlPacket();

    /**
     * Get the ID of the control packet type. Equates to {@link ControlPacketType#getTypeId()}.
     */
    int getControlPacketType();

    /**
     * Get the binary encoded form of the packet.
     */
    byte[] getEncoded();

    /**
     * Returns true if this packet should be routed to the {@link BoltSender}.
     */
    boolean forSender();

    /**
     * Get the packet sequence number.
     */
    int getPacketSeqNumber();

    @Override
    default int compareTo(BoltPacket o) {
        return SeqNum.comparePacketSeqNum(getPacketSeqNumber(), o.getPacketSeqNumber());
    }

}
