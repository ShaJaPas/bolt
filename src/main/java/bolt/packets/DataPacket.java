package bolt.packets;

import bolt.BoltPacket;
import bolt.BoltSession;

/**
 * The data packet header structure is as following:
 * <p>
 * <pre>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |0|                     Packet Sequence Number                  |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |FF |O|                     Message Number                      |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                          Time Stamp                           |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                    Destination Socket ID                      |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * </pre>
 * The data packet header starts with 0. Packet sequence number uses the
 * following 31 bits after the flag bit. UDT uses packet based
 * sequencing, i.e., the sequence number is increased by 1 for each sent
 * data packet in the order of packet sending. Sequence number is
 * wrapped after it is increased to the maximum number (2^31 - 1).
 * <p>
 * The next 32-bit field in the header is for the messaging. The first
 * two bits "FF" flags the position of the packet is a message. "10" is
 * the first packet, "01" is the last one, "11" is the only packet, and
 * "00" is any packets in the middle. The third bit "O" means if the
 * message should be delivered in order (1) or not (0). A message to be
 * delivered in order requires that all previous messages must be either
 * delivered or dropped. The rest 29 bits is the message number, similar
 * to packet sequence number (but independent). A UDT message may
 * contain multiple UDT packets.
 * <p>
 * Following are the 32-bit time stamp when the packet is sent and the
 * destination socket ID. The time stamp is a relative value starting
 * <p>
 * from the time when the connection is set up. The time stamp
 * information is not required by UDT or its native control algorithm.
 * It is included only in case that a user defined control algorithm may
 * require the information (See Section 6).
 * <p>
 * The Destination ID is used for UDP multiplexer. Multiple UDT socket
 * can be bound on the same UDP port and this UDT socket ID is used to
 * differentiate the UDT connections.
 */
public class DataPacket implements BoltPacket, Comparable<BoltPacket> {

    private byte[] data;
    private long packetSequenceNumber;
    private long messageNumber;

    //TODO remove timestamp
    private long timeStamp;
    private long destinationID;

    private BoltSession session;

    private int dataLength;

    public DataPacket() {
    }

    /**
     * Create a DataPacket from the given raw data.
     *
     * @param encodedData all bytes of packet including headers.
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
     * Complete header (16 bytes) + data packet for transmission
     */
    public byte[] getEncoded() {
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
