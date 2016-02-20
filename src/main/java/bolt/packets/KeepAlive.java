package bolt.packets;

public class KeepAlive extends ControlPacket {

    public KeepAlive() {
        this.controlPacketType = ControlPacketType.KEEP_ALIVE.getTypeId();
    }

    @Override
    public byte[] encodeControlInformation() {
        return null;
    }
}
