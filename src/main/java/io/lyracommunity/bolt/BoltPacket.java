package io.lyracommunity.bolt;


import io.lyracommunity.bolt.util.SeqNum;

public interface BoltPacket extends Comparable<BoltPacket> {


    int getDestinationID();

    void setDestinationID(int destinationID);

    boolean isControlPacket();

    int getControlPacketType();

    byte[] getEncoded();

    /**
     * Returns true if this packet should be routed to the {@link BoltSender}.
     */
    boolean forSender();

    int getPacketSeqNumber();

    @Override
    default int compareTo(BoltPacket o) {
        return SeqNum.comparePacketSeqNum(getPacketSeqNumber(), o.getPacketSeqNumber());
    }

}
