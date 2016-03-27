package io.lyracommunity.bolt;


public interface BoltPacket extends Comparable<BoltPacket> {


    int getMessageId();

    void setMessageId(int messageNumber);

    int getDestinationID();

    void setDestinationID(int destinationID);

    boolean isControlPacket();

    int getControlPacketType();

    byte[] getEncoded();

    /**
     * return <code>true</code> if this packet should be routed to
     * the {@link BoltSender}
     *
     * @return
     */
    boolean forSender();

    boolean isConnectionHandshake();

    BoltSession getSession();

    int getPacketSeqNumber();

}
