package io.lyracommunity.bolt.packet;

/**
 * Shutdown.
 * <p>
 * Additional Info: Undefined
 * <p>
 * Control Info: None
 */
public class Shutdown extends ControlPacket {

    Shutdown() {
        super(PacketType.SHUTDOWN);
    }

    public Shutdown(final int destinationID) {
        this();
        this.destinationID = destinationID;
    }

    @Override
    public byte[] encodeControlInformation() {
        return null;
    }

}

