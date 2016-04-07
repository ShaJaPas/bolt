package io.lyracommunity.bolt.packet;

import io.lyracommunity.bolt.receiver.Receiver;
import io.lyracommunity.bolt.sender.Sender;

import java.io.ByteArrayOutputStream;

/**
 * Acknowledgement (ACK) is sent by the {@link Receiver} to the {@link Sender}
 * to acknowledge receipt of packets.
 * <p>
 * Additional Info: ACK sequence number     <br>
 * Control Info:                            <br>
 * <ol>
 * <li> 32 bits: The packet sequence number to which all the
 * previous packets have been received (excluding)
 * <br> [The following fields are optional]
 * <li> 32 bits: RTT (in microseconds)
 * <li> 32 bits: RTT variance
 * <li> 32 bits: Available buffer size (in bytes)
 * <li> 32 bits: Packets receiving rate (in number of packets per second)
 * <li> 32 bits: Estimated link capacity (in number of packets per second)
 * </ol>
 */
public class Ack extends ControlPacket {

    /** The ACK sequence number */
    private int ackSequenceNumber;

    /** The packet sequence number to which all the previous packets have been received (excluding) */
    private int ackNumber;

    /** round-trip time in microseconds(RTT) */
    private long roundTripTime;

    /** RTT variance */
    private long roundTripTimeVariance;

    /** Available buffer size (in bytes) */
    private long bufferSize;

    /** Packet receiving rate in number of packets per second */
    private long pktArrivalSpeed;

    /** Estimated link capacity in number of packets per second */
    private long estimatedLinkCapacity;

    public Ack() {
        this.controlPacketType = PacketType.ACK.getTypeId();
    }

    public static Ack buildAcknowledgement(final int ackNumber, final int ackSequenceNumber,
            final long roundTripTime, final long roundTripTimeVar, final long bufferSize, final int destinationID,
            final long estimatedLinkCapacity, final long packetArrivalSpeed) {
        final Ack ack = buildLightAcknowledgement(ackNumber, ackSequenceNumber, roundTripTime, roundTripTimeVar, bufferSize, destinationID);
        ack.setEstimatedLinkCapacity(estimatedLinkCapacity);
        ack.setPacketReceiveRate(packetArrivalSpeed);
        return ack;
    }

    /**
     * Builds a "light" Acknowledgement.
     */
    public static Ack buildLightAcknowledgement(final int ackNumber, final int ackSequenceNumber,
            final long roundTripTime, final long roundTripTimeVar, final long bufferSize, final int destinationID) {
        Ack ack = new Ack();
        // The packet sequence number to which all the packets have been received
        ack.setAckNumber(ackNumber);
        // Assign this ack a unique increasing ACK sequence number
        ack.setAckSequenceNumber(ackSequenceNumber);
        ack.setRoundTripTime(roundTripTime);
        ack.setRoundTripTimeVar(roundTripTimeVar);
        ack.setBufferSize(bufferSize);
        ack.setDestinationID(destinationID);
        return ack;
    }

    public Ack(int ackSeqNo, byte[] controlInformation) {
        this();
        this.ackSequenceNumber = ackSeqNo;
        decodeControlInformation(controlInformation);
    }

    void decodeControlInformation(final byte[] data) {
        ackNumber = PacketUtil.decodeInt(data, 0);
        if (data.length > 4) {
            roundTripTime = PacketUtil.decode(data, 4);
            roundTripTimeVariance = PacketUtil.decode(data, 8);
            bufferSize = PacketUtil.decode(data, 12);
        }
        if (data.length > 16) {
            pktArrivalSpeed = PacketUtil.decode(data, 16);
            estimatedLinkCapacity = PacketUtil.decode(data, 20);
        }
    }

    @Override
    protected long getAdditionalInfo() {
        return ackSequenceNumber;
    }

    public long getAckSequenceNumber() {
        return ackSequenceNumber;
    }

    public void setAckSequenceNumber(final int ackSequenceNumber) {
        this.ackSequenceNumber = ackSequenceNumber;
    }


    /**
     * get the ack number (the number up to which all packets have been received (excluding))
     *
     * @return
     */
    public int getAckNumber() {
        return ackNumber;
    }

    /**
     * set the ack number (the number up to which all packets have been received (excluding))
     *
     * @param ackNumber
     */
    public void setAckNumber(int ackNumber) {
        this.ackNumber = ackNumber;
    }

    /**
     * get the round trip time (microseconds)
     *
     * @return
     */
    public long getRoundTripTime() {
        return roundTripTime;
    }

    /**
     * set the round trip time (in microseconds)
     *
     * @param RoundTripTime
     */
    public void setRoundTripTime(long RoundTripTime) {
        roundTripTime = RoundTripTime;
    }

    public long getRoundTripTimeVar() {
        return roundTripTimeVariance;
    }

    /**
     * set the variance of the round trip time (in microseconds)
     *
     * @param roundTripTimeVar
     */
    public void setRoundTripTimeVar(long roundTripTimeVar) {
        roundTripTimeVariance = roundTripTimeVar;
    }

    public long getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(long bufferSiZe) {
        this.bufferSize = bufferSiZe;
    }

    public long getPacketReceiveRate() {
        return pktArrivalSpeed;
    }

    public void setPacketReceiveRate(long packetReceiveRate) {
        this.pktArrivalSpeed = packetReceiveRate;
    }


    public long getEstimatedLinkCapacity() {
        return estimatedLinkCapacity;
    }

    public void setEstimatedLinkCapacity(long estimatedLinkCapacity) {
        this.estimatedLinkCapacity = estimatedLinkCapacity;
    }

    @Override
    public byte[] encodeControlInformation() {
        try {
            // TODO shorten packing
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(PacketUtil.encode(ackNumber));
            bos.write(PacketUtil.encode(roundTripTime));
            bos.write(PacketUtil.encode(roundTripTimeVariance));
            bos.write(PacketUtil.encode(bufferSize));
            bos.write(PacketUtil.encode(pktArrivalSpeed));
            bos.write(PacketUtil.encode(estimatedLinkCapacity));

            return bos.toByteArray();
        } catch (Exception e) {
            // can't happen
            return null;
        }
    }

    // TODO should ackSequenceNumber be included in this? Also, implement hashCode.
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        Ack other = (Ack) obj;
        if (ackNumber != other.ackNumber)
            return false;
        if (roundTripTime != other.roundTripTime)
            return false;
        if (roundTripTimeVariance != other.roundTripTimeVariance)
            return false;
        if (bufferSize != other.bufferSize)
            return false;
        if (estimatedLinkCapacity != other.estimatedLinkCapacity)
            return false;
        if (pktArrivalSpeed != other.pktArrivalSpeed)
            return false;
        return true;
    }


}
