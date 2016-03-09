package bolt.packets;

import bolt.BoltPacket;
import bolt.BoltSession;
import bolt.util.SequenceNumber;

import java.util.Arrays;
import java.util.Objects;

/**
 * The data packet header structure is as following:
 * <p>
 * <pre>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |0|M|R|                Packet Sequence Number                   |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |     Destination Socket ID     |            Class ID           |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |F|    Message Chunk Number     |           Message ID          |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * </pre>
 * The data packet header starts with 0.
 * <p>
 * M signifies whether the packet is a message.
 * <p>
 * R signifies whether delivery is reliable.
 * <p>
 * Packet sequence number uses the following 29 bits after the flag bits.
 * Bolt uses packet-based sequencing, i.e., the sequence number is increased
 * by 1 for each sent data packet in the order of packet sending. Sequence
 * number is wrapped after it is increased to the maximum number (2^29 - 1).
 * <p>
 * Following is the 32-bit Destination Socket ID.
 * The Destination ID is used for UDP multiplexer. Multiple UDT socket
 * can be bound on the same UDP port and this UDT socket ID is used to
 * differentiate the UDT connections.
 * <p>
 * The next 32-bit field in the header is for the messaging. The first bit
 * "F" flags whether the packet is the last message chunk (1), or not (0).
 * The Message Chunk Number is the position of this packet in the message.
 * The Message ID uniquely identifies this message from others.
 */
public class DataPacket implements BoltPacket, Comparable<BoltPacket> {

    private byte[] data;

    private boolean reliable;

    private boolean message;

    private int packetSequenceNumber;

    private boolean finalMessageChunk;

    private int messageChunkNumber;

    private int messageId;

    private int destinationID;

    private int classID;

    private int dataLength;

    private BoltSession session;

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

    void decode(final byte[] encodedData, final int length) {
        message = PacketUtil.isBitSet(encodedData[0], 6);
        reliable = PacketUtil.isBitSet(encodedData[0], 5);

        packetSequenceNumber = PacketUtil.decodeInt(encodedData, 0) & SequenceNumber.MAX_SEQ_NUM;
        destinationID = PacketUtil.decodeInt(encodedData, 4, 16, 32);
        classID = PacketUtil.decodeInt(encodedData, 4, 0, 16);

        // If is message.
        if (message) {
            finalMessageChunk = PacketUtil.isBitSet(encodedData[8], 7);
            messageChunkNumber = PacketUtil.decodeInt(encodedData, 8, 16, 31);
            messageId = PacketUtil.decodeInt(encodedData, 8, 0, 16);
        }

        final int headerLength = message ? 12 : 8;
        dataLength = length - headerLength;
        data = new byte[dataLength];
        System.arraycopy(encodedData, headerLength, data, 0, dataLength);
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

    public int getPacketSequenceNumber() {
        return this.packetSequenceNumber;
    }

    public void setPacketSequenceNumber(final int sequenceNumber) {
        this.packetSequenceNumber = sequenceNumber;
    }


    public int getMessageId() {
        return this.messageId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }

    public int getDestinationID() {
        return this.destinationID;
    }

    public void setDestinationID(int destinationID) {
        this.destinationID = destinationID;
    }

    /**
     * Complete header (12 bytes) + data packet for transmission
     */
    public byte[] getEncoded() {
        final int headerLength = message ? 12 : 8;
        byte[] result = new byte[headerLength + dataLength];

        byte[] flagsAndSeqNum = PacketUtil.encodeInt(packetSequenceNumber);
        flagsAndSeqNum[0] = PacketUtil.setBit(flagsAndSeqNum[0], 7, false);
        flagsAndSeqNum[0] = PacketUtil.setBit(flagsAndSeqNum[0], 6, message);
        flagsAndSeqNum[0] = PacketUtil.setBit(flagsAndSeqNum[0], 5, reliable);

        System.arraycopy(flagsAndSeqNum, 0, result, 0, 4);
        System.arraycopy(PacketUtil.encode(destinationID), 2, result, 4, 2);
        System.arraycopy(PacketUtil.encode(classID), 2, result, 6, 2);
        if (message) {
            final byte[] messageBits = new byte[4];
            messageBits[0] = PacketUtil.setBit(messageBits[0], 7, finalMessageChunk);
            PacketUtil.encodeMapToBytes(messageChunkNumber, messageBits, 15, 15);
            PacketUtil.encodeMapToBytes(messageId, messageBits, 31, 16);
            System.arraycopy(messageBits, 0, result, 8, 4);
        }
        System.arraycopy(data, 0, result, headerLength, dataLength);
        return result;
    }

    public void copyFrom(final DataPacket src) {
        setClassID(src.getClassID());
        setPacketSequenceNumber(src.getPacketSequenceNumber());
        setSession(src.getSession());
        setDestinationID(src.getDestinationID());

        setData(src.getData());
        setMessage(src.isMessage());
        setReliable(src.isReliable());
        setFinalMessageChunk(src.isFinalMessageChunk());
        setMessageChunkNumber(src.getMessageChunkNumber());
        setMessageId(src.getMessageId());
    }

    public int getClassID() {
        return classID;
    }

    public void setClassID(int classID) {
        this.classID = classID;
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

    public boolean isReliable() {
        return reliable;
    }

    public void setReliable(boolean reliable) {
        this.reliable = reliable;
    }

    public boolean isMessage() {
        return message;
    }

    public void setMessage(boolean message) {
        this.message = message;
    }

    public int getMessageChunkNumber() {
        return messageChunkNumber;
    }

    public void setMessageChunkNumber(int messageChunkNumber) {
        this.messageChunkNumber = messageChunkNumber;
    }

    public boolean isFinalMessageChunk() {
        return finalMessageChunk;
    }

    public void setFinalMessageChunk(boolean finalMessageChunk) {
        this.finalMessageChunk = finalMessageChunk;
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
        return (getPacketSequenceNumber() - other.getPacketSequenceNumber());
    }

    public boolean isClassful() {
        return classID > 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataPacket that = (DataPacket) o;
        return reliable == that.reliable &&
                message == that.message &&
                packetSequenceNumber == that.packetSequenceNumber &&
                finalMessageChunk == that.finalMessageChunk &&
                messageChunkNumber == that.messageChunkNumber &&
                messageId == that.messageId &&
                destinationID == that.destinationID &&
                classID == that.classID &&
                Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, reliable, message, packetSequenceNumber, finalMessageChunk, messageChunkNumber, messageId, destinationID, classID);
    }

    @Override
    public String toString() {
        return "DataPacket{" +
                "classID=" + classID +
                ", data=" + Arrays.toString(data) +
                ", reliable=" + reliable +
                ", message=" + message +
                ", packetSequenceNumber=" + packetSequenceNumber +
                ", finalMessageChunk=" + finalMessageChunk +
                ", messageChunkNumber=" + messageChunkNumber +
                ", messageId=" + messageId +
                ", destinationID=" + destinationID +
                '}';
    }
}
