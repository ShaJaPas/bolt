package bolt.packets;

import bolt.BoltPacket;
import bolt.packets.ControlPacket.ControlPacketType;

import java.io.IOException;

public class PacketFactory {

    /**
     * Creates a Control or Data packet depending on the highest bit
     * of the first 32 bit of data.
     *
     * @param encodedData encoded data to decode.
     * @return the created packet.
     */
    public static BoltPacket createPacket(byte[] encodedData) throws IOException {
        boolean isControl = (encodedData[0] & 128) != 0;
        if (isControl) return createControlPacket(encodedData, encodedData.length);
        return new DataPacket(encodedData);
    }

    public static BoltPacket createPacket(byte[] encodedData, int length) throws IOException {
        boolean isControl = (encodedData[0] & 128) != 0;
        if (isControl) return createControlPacket(encodedData, length);
        return new DataPacket(encodedData, length);
    }

    /**
     * Create the right type of control packet based on the packet data.
     *
     * @param encodedData the encoded control packet.
     * @param length size of the encoded packet.
     * @return the created Control packet.
     */
    public static ControlPacket createControlPacket(byte[] encodedData, int length) throws IOException {

        ControlPacket packet = null;

        int pktType = PacketUtil.decodeType(encodedData, 0);
        int additionalInfo = PacketUtil.decodeInt(encodedData, 4);
        int destID = PacketUtil.decodeInt(encodedData, 8);
        byte[] controlInformation = new byte[length - 12];
        System.arraycopy(encodedData, 12, controlInformation, 0, controlInformation.length);

        if (ControlPacketType.CONNECTION_HANDSHAKE.getTypeId() == pktType) {
            packet = new ConnectionHandshake(controlInformation);
        }
        else if (ControlPacketType.KEEP_ALIVE.getTypeId() == pktType) {
            packet = new KeepAlive();
        }
        else if (ControlPacketType.ACK.getTypeId() == pktType) {
            packet = new Acknowledgement(additionalInfo, controlInformation);
        }
        else if (ControlPacketType.NAK.getTypeId() == pktType) {
            packet = new NegativeAcknowledgement(controlInformation);
            if (((NegativeAcknowledgement)packet).getDecodedLossInfo().isEmpty()) {
                System.out.println("NO");
            }
        }
        else if (ControlPacketType.SHUTDOWN.getTypeId() == pktType) {
            packet = new Shutdown();
        }
        else if (ControlPacketType.ACK2.getTypeId() == pktType) {
            packet = new Acknowledgment2(additionalInfo, controlInformation);
        }
        else if (ControlPacketType.MESSAGE_DROP_REQUEST.getTypeId() == pktType) {
            packet = new MessageDropRequest(controlInformation);
        }
        else if (ControlPacketType.USER_DEFINED.getTypeId() == pktType) {
            packet = new UserDefined(controlInformation);
        }

        if (packet != null) {
            packet.setDestinationID(destID);
        }
        return packet;

    }

}
