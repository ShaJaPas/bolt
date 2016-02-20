package bolt.packets;


public class Shutdown extends ControlPacket {

    public Shutdown() {
        this.controlPacketType = ControlPacketType.SHUTDOWN.getTypeId();
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

