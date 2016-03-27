package io.lyracommunity.bolt.packet;


public class UserDefined extends ControlPacket {

    public UserDefined() {
        controlPacketType = ControlPacketType.USER_DEFINED.getTypeId();
    }

    // Explained by bits 4-15. Reserved for user defined Control Packet
    public UserDefined(byte[] controlInformation) {
        this.controlInformation = controlInformation;
    }

    @Override
    public byte[] encodeControlInformation() {
        return null;
    }
}
