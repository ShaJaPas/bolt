package bolt.packets;

import bolt.BoltPacket;
import bolt.BoltSession;

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
        USER_DEFINED(15),
        ;

        private final int typeId;

        ControlPacketType(int typeId) {
            this.typeId = typeId;
        }

        public int getTypeId() {
            return typeId;
        }

    }

}
