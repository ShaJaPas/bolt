package bolt.packets;

import bolt.BoltPacket;
import bolt.BoltSession;

public class DataPacket implements BoltPacket, Comparable<BoltPacket> {

    private byte[] data;
    private long packetSequenceNumber;
    private long messageNumber;
    private long timeStamp;
    private long destinationID;

    private BoltSession session;

    private int dataLength;

    public DataPacket() {
    }

    /**
     * create a DataPacket from the given raw data
     *
     * @param encodedData - network data
     */
    public DataPacket(byte[] encodedData) {
        this(encodedData, encodedData.length);
    }

    public DataPacket(byte[] encodedData, int length) {
        decode(encodedData, length);
    }

    void decode(byte[] encodedData, int length) {
        packetSequenceNumber = PacketUtil.decode(encodedData, 0);
        messageNumber = PacketUtil.decode(encodedData, 4);
        timeStamp = PacketUtil.decode(encodedData, 8);
        destinationID = PacketUtil.decode(encodedData, 12);
        dataLength = length - 16;
        data = new byte[dataLength];
        System.arraycopy(encodedData, 16, data, 0, dataLength);
    }


    public byte[] getData() {
        return this.data;
    }

    public void setData(byte[] data) {
        this.data = data;
        dataLength = data.length;
    }

    public int getLength() {
        return dataLength;
    }

    public void setLength(int length) {
        dataLength = length;
    }

    public long getPacketSequenceNumber() {
        return this.packetSequenceNumber;
    }

    public void setPacketSequenceNumber(long sequenceNumber) {
        this.packetSequenceNumber = sequenceNumber;
    }


    public long getMessageNumber() {
        return this.messageNumber;
    }

    public void setMessageNumber(long messageNumber) {
        this.messageNumber = messageNumber;
    }

    public long getDestinationID() {
        return this.destinationID;
    }

    public void setDestinationID(long destinationID) {
        this.destinationID = destinationID;
    }

    public long getTimeStamp() {
        return this.timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    /**
     * complete header+data packet for transmission
     */
    public byte[] getEncoded() {
        //header.length is 16
        byte[] result = new byte[16 + dataLength];
        System.arraycopy(PacketUtil.encode(packetSequenceNumber), 0, result, 0, 4);
        System.arraycopy(PacketUtil.encode(messageNumber), 0, result, 4, 4);
        System.arraycopy(PacketUtil.encode(timeStamp), 0, result, 8, 4);
        System.arraycopy(PacketUtil.encode(destinationID), 0, result, 12, 4);
        System.arraycopy(data, 0, result, 16, dataLength);
        return result;
    }

    public boolean isControlPacket() {
        return false;
    }

    public boolean forSender() {
        return false;
    }

    public boolean isConnectionHandshake() {
        return false;
    }

    public int getControlPacketType() {
        return -1;
    }

    public BoltSession getSession() {
        return session;
    }

    public void setSession(BoltSession session) {
        this.session = session;
    }

    public int compareTo(BoltPacket other) {
        return (int) (getPacketSequenceNumber() - other.getPacketSequenceNumber());
    }
}
