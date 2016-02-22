package bolt.packets;

import bolt.BoltPacket;
import bolt.BoltSession;

/**
 * If the flag bit of a UDT packet is 1, then it is a control packet and
 * parsed according to the following structure:
 * <p>
 * <pre>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |1|             Type            |            Reserved           |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |     |                    Additional Info                      |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                            Time Stamp                         |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                    Destination Socket ID                      |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                                                               |
 * ~                 Control Information Field                     ~
 * |                                                               |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * </pre>
 * <p>
 * There are 8 types of control packets in UDT and the type information
 * is put in bit field 1 - 15 of the header. The contents of the
 * following fields depend on the packet type. The first 128 bits must
 * exist in the packet header, whereas there may be an empty control
 * information field, depending on the packet type.
 * <p>
 * Particularly, UDT uses sub-sequencing for ACK packet. Each ACK packet
 * is assigned a unique increasing 16-bit sequence number, which is
 * independent of the data packet sequence number. The ACK sequence
 * number uses bits 32 - 63 ("Additional Info") in the control packet
 * header. The ACK sequence number ranges from 0 to (2^31 - 1).
 * <p>
 * TYPE 0x0:  Protocol Connection Handshake
 * Additional Info: Undefined
 * Control Info:
 * 1) 32 bits: UDT version
 * 2) 32 bits: Socket Type (STREAM or DGRAM)
 * 3) 32 bits: initial packet sequence number
 * 4) 32 bits: maximum packet size (including UDP/IP headers)
 * 5) 32 bits: maximum flow window size
 * 6) 32 bits: connection type (regular or rendezvous)
 * 7) 32 bits: socket ID
 * 8) 32 bits: SYN cookie
 * 9) 128 bits: the IP address of the peer's UDP socket
 * <p>
 * TYPE 0x1:  Keep-alive
 * Additional Info: Undefined
 * Control Info: None
 * <p>
 * TYPE 0x2:  Acknowledgement (ACK)
 * Additional Info: ACK sequence number
 * Control Info:
 * 1) 32 bits: The packet sequence number to which all the
 * previous packets have been received (excluding)
 * [The following fields are optional]
 * 2) 32 bits: RTT (in microseconds)
 * 3) 32 bits: RTT variance
 * 4) 32 bits: Available buffer size (in bytes)
 * 5) 32 bits: Packets receiving rate (in number of packets
 * per second)
 * 6) 32 bits: Estimated link capacity (in number of packets
 * per second)
 * <p>
 * TYPE 0x3:  Negative Acknowledgement (NAK)
 * Additional Info: Undefined
 * Control Info:
 * 1) 32 bits integer array of compressed loss information
 * (see section 3.9).
 * <p>
 * TYPE 0x4:  Unused
 * <p>
 * TYPE 0x5:  Shutdown
 * Additional Info: Undefined
 * Control Info: None
 * <p>
 * TYPE 0x6:  Acknowledgement of Acknowledgement (ACK2)
 * Additional Info: ACK sequence number
 * Control Info: None
 * <p>
 * TYPE 0x7:  Message Drop Request:
 * Additional Info: Message ID
 * Control Info:
 * 1) 32 bits: First sequence number in the message
 * 2) 32 bits: Last sequence number in the message
 * <p>
 * TYPE 0x7FFF: Explained by bits 16 - 31, reserved for user defined
 * Control Packet
 * <p>
 * Finally, Time Stamp and Destination Socket ID also exist in the
 * control packets.
 */
public abstract class ControlPacket implements BoltPacket {

    protected int controlPacketType;

    protected long messageNumber;

    protected long timeStamp;

    protected long destinationID;

    protected byte[] controlInformation;

    private BoltSession session;

    public ControlPacket() {

    }

    public int getControlPacketType() {
        return controlPacketType;
    }

    public long getMessageNumber() {
        return messageNumber;
    }

    public void setMessageNumber(long messageNumber) {
        this.messageNumber = messageNumber;
    }


    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }


    public long getDestinationID() {
        return destinationID;
    }

    public void setDestinationID(long destinationID) {
        this.destinationID = destinationID;
    }


    /**
     * return the header according to specification p.5
     *
     * @return
     */
    byte[] getHeader() {
        byte[] res = new byte[16];
        System.arraycopy(PacketUtil.encodeControlPacketType(controlPacketType), 0, res, 0, 4);
        System.arraycopy(PacketUtil.encode(getAdditionalInfo()), 0, res, 4, 4);
        System.arraycopy(PacketUtil.encode(timeStamp), 0, res, 8, 4);
        System.arraycopy(PacketUtil.encode(destinationID), 0, res, 12, 4);
        return res;
    }

    /**
     * this method gets the "additional info" for this type of control packet
     */
    protected long getAdditionalInfo() {
        return 0L;
    }


    /**
     * this method builds the control information
     * from the control parameters
     *
     * @return
     */
    public abstract byte[] encodeControlInformation();

    /**
     * complete header+ControlInformation packet for transmission
     */

    public byte[] getEncoded() {
        byte[] header = getHeader();
        byte[] controlInfo = encodeControlInformation();
        byte[] result = controlInfo != null ?
                new byte[header.length + controlInfo.length] :
                new byte[header.length];
        System.arraycopy(header, 0, result, 0, header.length);
        if (controlInfo != null) {
            System.arraycopy(controlInfo, 0, result, header.length, controlInfo.length);
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ControlPacket other = (ControlPacket) obj;
        if (controlPacketType != other.controlPacketType)
            return false;
        if (destinationID != other.destinationID)
            return false;
        if (timeStamp != other.timeStamp)
            return false;
        return true;
    }

    public boolean isControlPacket() {
        return true;
    }

    public boolean forSender() {
        return true;
    }

    public boolean isConnectionHandshake() {
        return false;
    }

    public BoltSession getSession() {
        return session;
    }

    public void setSession(BoltSession session) {
        this.session = session;
    }

    public long getPacketSequenceNumber() {
        return -1;
    }

    public int compareTo(BoltPacket other) {
        return (int) (getPacketSequenceNumber() - other.getPacketSequenceNumber());
    }

    public enum ControlPacketType {

        CONNECTION_HANDSHAKE(0),
        KEEP_ALIVE(1),
        ACK(2),
        NAK(3),
        UNUNSED_1(4),
        SHUTDOWN(5),
        ACK2(6),
        MESSAGE_DROP_REQUEST(7),
        UNUNSED_2(8),
        UNUNSED_3(9),
        UNUNSED_4(10),
        UNUNSED_5(11),
        UNUNSED_6(12),
        UNUNSED_7(13),
        UNUNSED_8(14),
        USER_DEFINED(15),;

        private final int typeId;

        ControlPacketType(int typeId) {
            this.typeId = typeId;
        }

        public int getTypeId() {
            return typeId;
        }

    }

}
