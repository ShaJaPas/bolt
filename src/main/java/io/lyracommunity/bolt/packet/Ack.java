package io.lyracommunity.bolt.packet;

import io.lyracommunity.bolt.receiver.Receiver;
import io.lyracommunity.bolt.sender.Sender;

import java.io.ByteArrayOutputStream;
import java.util.Objects;

/**
 * Acknowledgement (ACK) is sent by the {@link Receiver} to the {@link Sender}
 * to acknowledge receipt of packets.
 * <p>
 * Additional Info: ACK sequence number
 * <p>
 * Control Info:
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

    Ack() {
        super(PacketType.ACK);
    }

    Ack(int ackSeqNo, byte[] controlInformation) {
        super(PacketType.ACK, controlInformation);
        this.ackSequenceNumber = ackSeqNo;
        decodeControlInformation(controlInformation);
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

    private void decodeControlInformation(final byte[] data) {
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

    void setAckSequenceNumber(final int ackSequenceNumber) {
        this.ackSequenceNumber = ackSequenceNumber;
    }

    /**
     * @return the ACK number (the number up to which all packets have been received, exclusive).
     */
    public int getAckNumber() {
        return ackNumber;
    }

    /**
     * Set the ack number (the number up to which all packets have been received, exclusive).
     *
     * @param ackNumber the ack number as described above.
     */
    void setAckNumber(final int ackNumber) {
        this.ackNumber = ackNumber;
    }

    /**
     * @return the round trip time, in microseconds.
     */
    public long getRoundTripTime() {
        return roundTripTime;
    }

    /**
     * Set the round trip time, in microseconds.
     *
     * @param roundTripTime the value to set.
     */
    void setRoundTripTime(final long roundTripTime) {
        this.roundTripTime = roundTripTime;
    }

    public long getRoundTripTimeVar() {
        return roundTripTimeVariance;
    }

    /**
     * Set the variance of the round trip time, in microseconds.
     *
     * @param roundTripTimeVar the variance as described above.
     */
    void setRoundTripTimeVar(final long roundTripTimeVar) {
        roundTripTimeVariance = roundTripTimeVar;
    }

    void setBufferSize(final long bufferSiZe) {
        this.bufferSize = bufferSiZe;
    }

    public long getPacketReceiveRate() {
        return pktArrivalSpeed;
    }

    void setPacketReceiveRate(final long packetReceiveRate) {
        this.pktArrivalSpeed = packetReceiveRate;
    }

    public long getEstimatedLinkCapacity() {
        return estimatedLinkCapacity;
    }

    void setEstimatedLinkCapacity(final long estimatedLinkCapacity) {
        this.estimatedLinkCapacity = estimatedLinkCapacity;
    }

    @Override
    public byte[] encodeControlInformation() {
        try {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        final Ack ack = (Ack) o;
        return ackSequenceNumber == ack.ackSequenceNumber &&
                ackNumber == ack.ackNumber &&
                roundTripTime == ack.roundTripTime &&
                roundTripTimeVariance == ack.roundTripTimeVariance &&
                bufferSize == ack.bufferSize &&
                pktArrivalSpeed == ack.pktArrivalSpeed &&
                estimatedLinkCapacity == ack.estimatedLinkCapacity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), ackSequenceNumber, ackNumber, roundTripTime, roundTripTimeVariance,
                bufferSize, pktArrivalSpeed, estimatedLinkCapacity);
    }

    @Override
    public String toString() {
        return "Ack{" +
                "ackSequenceNumber=" + ackSequenceNumber +
                ", ackNumber=" + ackNumber +
                ", roundTripTime=" + roundTripTime +
                ", roundTripTimeVariance=" + roundTripTimeVariance +
                ", bufferSize=" + bufferSize +
                ", pktArrivalSpeed=" + pktArrivalSpeed +
                ", estimatedLinkCapacity=" + estimatedLinkCapacity +
                '}';
    }

}
