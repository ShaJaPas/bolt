package bolt.packets;

import bolt.BoltPacket;
import bolt.packets.ControlPacket.ControlPacketType;

import java.io.IOException;

public class PacketFactory {

    /**
     * creates a Control or Data packet depending on the highest bit
     * of the first 32 bit of data
     *
     * @param encodedData
     * @return
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
     * create the right type of control packet based on the packet data
     *
     * @param encodedData
     * @param length
     * @return
     */
    public static ControlPacket createControlPacket(byte[] encodedData, int length) throws IOException {

        ControlPacket packet = null;

        int pktType = PacketUtil.decodeType(encodedData, 0);
        long additionalInfo = PacketUtil.decode(encodedData, 4);
        long timeStamp = PacketUtil.decode(encodedData, 8);
        long destID = PacketUtil.decode(encodedData, 12);
        byte[] controlInformation = new byte[length - 16];
        System.arraycopy(encodedData, 16, controlInformation, 0, controlInformation.length);

        //TYPE 0000:0
        if (ControlPacketType.CONNECTION_HANDSHAKE.getTypeId() == pktType) {
            packet = new ConnectionHandshake(controlInformation);
        }
        //TYPE 0001:1
        else if (ControlPacketType.KEEP_ALIVE.getTypeId() == pktType) {
            packet = new KeepAlive();
        }
        //TYPE 0010:2
        else if (ControlPacketType.ACK.getTypeId() == pktType) {
            packet = new Acknowledgement(additionalInfo, controlInformation);
        }
        //TYPE 0011:3
        else if (ControlPacketType.NAK.getTypeId() == pktType) {
            packet = new NegativeAcknowledgement(controlInformation);
        }
        //TYPE 0101:5
        else if (ControlPacketType.SHUTDOWN.getTypeId() == pktType) {
            packet = new Shutdown();
        }
        //TYPE 0110:6
        else if (ControlPacketType.ACK2.getTypeId() == pktType) {
            packet = new Acknowledgment2(additionalInfo, controlInformation);
        }
        //TYPE 0111:7
        else if (ControlPacketType.MESSAGE_DROP_REQUEST.getTypeId() == pktType) {
            packet = new MessageDropRequest(controlInformation);
        }
        //TYPE 1111:8
        else if (ControlPacketType.USER_DEFINED.getTypeId() == pktType) {
            packet = new UserDefined(controlInformation);
        }

        if (packet != null) {
            packet.setTimeStamp(timeStamp);
            packet.setDestinationID(destID);
        }
        return packet;

    }

}
