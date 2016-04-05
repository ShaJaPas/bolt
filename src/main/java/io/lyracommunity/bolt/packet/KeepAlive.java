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
        this.controlPacketType = PacketType.KEEP_ALIVE.getTypeId();
    }

    @Override
    public byte[] encodeControlInformation() {
        return null;
    }

}
