package io.lyracommunity.bolt.packet;

/**
 * Created by keen on 28/03/16.
 */
public enum ControlPacketType {

    CONNECTION_HANDSHAKE(0),
    KEEP_ALIVE(1),
    ACK(2),
    NAK(3),
    SHUTDOWN(4),
    ACK2(5);

    private final int typeId;

    ControlPacketType(int typeId) {
        this.typeId = typeId;
    }

    public int getTypeId() {
        return typeId;
    }

}
