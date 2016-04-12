package io.lyracommunity.bolt.packet;

/**
 * Keep-alive
 * <p>
 * Additional Info: Undefined
 * <p>
 * Control Info: None
 */
public class KeepAlive extends ControlPacket {

    public KeepAlive() {
        super(PacketType.KEEP_ALIVE);
    }

    @Override
    public byte[] encodeControlInformation() {
        return null;
    }

}
