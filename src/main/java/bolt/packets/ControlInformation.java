package bolt.packets;

import bolt.packets.ControlPacket.ControlPacketType;

public interface ControlInformation {

    byte[] getEncodedControlInformation();

    ControlPacketType getType();
}
