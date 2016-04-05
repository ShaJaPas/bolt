package io.lyracommunity.bolt.packet;

import java.io.IOException;

public class PacketFactory {


    static BoltPacket createPacket(byte[] encodedData) throws IOException {
        return createPacket(encodedData, encodedData.length);
    }

    /**
     * Creates a Control or Data packet depending on the highest bit
     * of the first 32 bit of data.
     *
     * @param encodedData encoded data to decode.
     * @param length length of the encoded data.
     * @return the created packet.
     */
    public static BoltPacket createPacket(byte[] encodedData, int length) throws IOException {
        boolean isControl = (encodedData[0] & 128) != 0;
        if (isControl) return createControlPacket(encodedData, length);
        return new DataPacket(encodedData, length);
    }

    /**
     * Create the right type of control packet based on the packet data.
     *
     * @param encodedData the encoded control packet.
     * @param length      size of the encoded packet.
     * @return the created Control packet.
     */
    private static ControlPacket createControlPacket(byte[] encodedData, int length) throws IOException {

        ControlPacket packet = null;

        int pktType = PacketUtil.decodeType(encodedData, 0);
        int additionalInfo = PacketUtil.decodeInt(encodedData, 4);
        int destID = PacketUtil.decodeInt(encodedData, 8);
        byte[] controlInformation = new byte[length - 12];
        System.arraycopy(encodedData, 12, controlInformation, 0, controlInformation.length);

        if (PacketType.CONNECTION_HANDSHAKE.getTypeId() == pktType) {
            packet = new ConnectionHandshake(controlInformation);
        }
        else if (PacketType.KEEP_ALIVE.getTypeId() == pktType) {
            packet = new KeepAlive();
        }
        else if (PacketType.ACK.getTypeId() == pktType) {
            packet = new Ack(additionalInfo, controlInformation);
        }
        else if (PacketType.NAK.getTypeId() == pktType) {
            packet = new NegAck(controlInformation);
            if (((NegAck) packet).getDecodedLossInfo().isEmpty()) {
                System.out.println("NO");
            }
        }
        else if (PacketType.SHUTDOWN.getTypeId() == pktType) {
            packet = new Shutdown();
        }
        else if (PacketType.ACK2.getTypeId() == pktType) {
            packet = new Ack2(additionalInfo, controlInformation);
        }

        if (packet != null) {
            packet.setDestinationID(destID);
        }
        return packet;

    }

}
