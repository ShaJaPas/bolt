package io.lyracommunity.bolt.packet;

/**
 * Shutdown.
 * <p>
 * Additional Info: Undefined
 * <p>
 * Control Info: None
 */
public class Shutdown extends ControlPacket {

    public Shutdown() {
        this.controlPacketType = PacketType.SHUTDOWN.getTypeId();
    }

    public Shutdown(final int destinationID) {
        this();
        this.destinationID = destinationID;
    }

    @Override
    public byte[] encodeControlInformation() {
        return null;
    }

    @Override
    public boolean forSender() {
        return false;
    }

}

