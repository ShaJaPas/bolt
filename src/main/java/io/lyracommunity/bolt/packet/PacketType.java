package io.lyracommunity.bolt.packet;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by keen on 28/03/16.
 */
public enum PacketType {

    DATA(-1),
    CONNECTION_HANDSHAKE(0),
    KEEP_ALIVE(1),
    ACK(2),
    NAK(3),
    SHUTDOWN(4),
    ACK2(5);

    private final int typeId;

    PacketType(final int typeId) {
        this.typeId = typeId;
    }

    public int getTypeId() {
        return typeId;
    }

    public static PacketType byTypeId(final int typeId) {
        return TYPE_ID_MAPPING.get(typeId);
    }

    // Lookup tables and functions below here.
    private static Map<Integer, PacketType> TYPE_ID_MAPPING = buildTypeIdMapping();

    private static Map<Integer, PacketType> buildTypeIdMapping() {
        return Stream.of(PacketType.values())
                .collect(Collectors.toMap(PacketType::getTypeId, Function.identity()));
    }

}
