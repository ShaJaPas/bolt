package io.lyracommunity.bolt.packet;


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
     * Get the {@link PacketType packet type}.
     */
    PacketType getPacketType();

    /**
     * Get the binary encoded form of the packet.
     */
    byte[] getEncoded();

    /**
     * Get the packet sequence number.
     */
    int getPacketSeqNumber();

    @Override
    default int compareTo(BoltPacket o) {
        return SeqNum.comparePacketSeqNum(getPacketSeqNumber(), o.getPacketSeqNumber());
    }

}
