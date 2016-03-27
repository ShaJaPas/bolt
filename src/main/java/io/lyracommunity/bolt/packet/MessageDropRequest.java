package io.lyracommunity.bolt.packet;

import java.io.ByteArrayOutputStream;
import java.util.Objects;

/**
 * Message Drop Request.
 * <p>
 * Additional Info: Message ID
 * <p>
 * Control Info:
 * <ol>
 * <li> 32 bits: First sequence number in the message
 * <li> 32 bits: Last sequence number in the message
 * </ol>
 */
public class MessageDropRequest extends ControlPacket {
    // Bits 35-64: Message number

    private long msgFirstSeqNo;
    private long msgLastSeqNo;

    public MessageDropRequest() {
        this.controlPacketType = ControlPacketType.MESSAGE_DROP_REQUEST.getTypeId();
    }

    public MessageDropRequest(byte[] controlInformation) {
        this();
        decode(controlInformation);
    }

    void decode(byte[] data) {
        msgFirstSeqNo = PacketUtil.decode(data, 0);
        msgLastSeqNo = PacketUtil.decode(data, 4);
    }

    public long getMsgFirstSeqNo() {
        return msgFirstSeqNo;
    }

    public void setMsgFirstSeqNo(long msgFirstSeqNo) {
        this.msgFirstSeqNo = msgFirstSeqNo;
    }

    public long getMsgLastSeqNo() {
        return msgLastSeqNo;
    }

    public void setMsgLastSeqNo(long msgLastSeqNo) {
        this.msgLastSeqNo = msgLastSeqNo;
    }

    @Override
    public byte[] encodeControlInformation() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(PacketUtil.encode(msgFirstSeqNo));
            bos.write(PacketUtil.encode(msgLastSeqNo));
            return bos.toByteArray();
        } catch (Exception e) {
            // can't happen
            return null;
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        MessageDropRequest that = (MessageDropRequest) o;
        return msgFirstSeqNo == that.msgFirstSeqNo && msgLastSeqNo == that.msgLastSeqNo;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), msgFirstSeqNo, msgLastSeqNo);
    }

}
