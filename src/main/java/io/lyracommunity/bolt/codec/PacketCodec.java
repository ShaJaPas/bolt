package io.lyracommunity.bolt.codec;

import io.lyracommunity.bolt.api.BoltException;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.packet.DataPacket;
import io.lyracommunity.bolt.packet.DeliveryType;
import io.lyracommunity.bolt.packet.PacketUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Created by omahoc9 on 3/1/16.
 */
public class PacketCodec<T> implements Codec<T, List<DataPacket>> {

    // TODO consider changing 1400 to a variable MTU
    private final int maxPacketSize = Config.DEFAULT_DATAGRAM_SIZE - DataPacket.MAX_HEADER_SIZE;

    private final ObjectCodec<T> objectCodec;

    private DeliveryType deliveryType;

    public PacketCodec(final ObjectCodec<T> objectCodec) {
        this(objectCodec, DeliveryType.RELIABLE_ORDERED_MESSAGE);
    }

    public PacketCodec(final ObjectCodec<T> objectCodec, final DeliveryType deliveryType) {
        Objects.requireNonNull(objectCodec);
        this.objectCodec = objectCodec;
        this.deliveryType = deliveryType;
    }

    /**
     * Decodes a packet back into its original object.
     *
     * @param data the data packet to decode.
     * @return decoded object, or null if packet was a chunk of a yet incomplete message.
     */
    @Override
    public T decode(final List<DataPacket> data) {
        if (data.size() == 1) {
            return objectCodec.decode(data.get(0).getData());
        }
        else {
            // Get total bytes in object.
            int byteCount = 0;
            for (DataPacket p : data) byteCount += p.getDataLength();

            // Copy all data packets into single byte array.
            final byte[] bytes = new byte[byteCount];
            int destPos = 0;
            for (DataPacket p : data) {
                System.arraycopy(p.getData(), 0, bytes, destPos, p.getDataLength());
                destPos += p.getDataLength();
            }

            // Decode byte array into object.
            return objectCodec.decode(bytes);
        }
    }

    /**
     * Encodes an object into a series of data packets.
     *
     * @param object the object to encode.
     * @return a collection of data packets.
     */
    @Override
    public List<DataPacket> encode(final T object) throws BoltException {
        final byte[] bytes = objectCodec.encode(object);
        final int chunkCount = Math.max(1, (int) Math.ceil(bytes.length / (double) maxPacketSize));
        final DeliveryType computedDeliveryType = computeDeliveryType(chunkCount);

        validateEncoding(chunkCount, computedDeliveryType);

        final List<DataPacket> dataPackets = new ArrayList<>(chunkCount);
        for (int i = 0; i < chunkCount; i++) {
            final int byteOffset = i * maxPacketSize;
            final int packetSize = Math.min(maxPacketSize, bytes.length - byteOffset);
            final byte[] packetData = new byte[packetSize];
            System.arraycopy(bytes, byteOffset, packetData, 0, packetSize);

            final DataPacket packet = new DataPacket();
            packet.setData(packetData);
            packet.setDelivery(computedDeliveryType);
            packet.setClassID(objectCodec.getClassId());
            if (computedDeliveryType.isMessage()) {
                packet.setMessageChunkNumber(i);
                packet.setFinalMessageChunk(i == (chunkCount - 1));
            }
            dataPackets.add(packet);
        }

        return dataPackets;
    }

    private void validateEncoding(int chunkCount, DeliveryType computedDeliveryType) throws BoltException {
        if (chunkCount > PacketUtil.MAX_MESSAGE_CHUNK_NUM) {
            throw new BoltException("Object is too large to chunk. Actual chunk count: " + chunkCount);
        }
        else if (chunkCount > 1 && (!computedDeliveryType.isMessage())) {
            throw new BoltException("Packet over the maximum size and not a message. Chunk count: " + chunkCount);
        }
    }

    private DeliveryType computeDeliveryType(final int chunkCount) {
        return (chunkCount > 1) ? deliveryType : deliveryType.toNonMessage();
    }

    public void setClassId(final int classId) {
        objectCodec.setClassId(classId);
    }

}
