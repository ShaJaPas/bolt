package bolt.packets;

/**
 * Keep-alive
 * <p>
 * Additional Info: Undefined   <br>
 * Control Info: None           <br>
 */
public class KeepAlive extends ControlPacket {

    public KeepAlive() {
        this.controlPacketType = ControlPacketType.KEEP_ALIVE.getTypeId();
    }

    @Override
    public byte[] encodeControlInformation() {
        return null;
    }
}
