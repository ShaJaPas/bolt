package bolt.packets;


public class UserDefined extends ControlPacket {

    public UserDefined() {
        controlPacketType = ControlPacketType.USER_DEFINED.getTypeId();
    }

    //Explained by bits 4-15,
    //reserved for user defined Control Packet
    public UserDefined(byte[] controlInformation) {
        this.controlInformation = controlInformation;
    }

    @Override
    public byte[] encodeControlInformation() {
        return null;
    }
}
