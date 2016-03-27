package bolt.codec;

import bolt.BoltEndPoint;
import bolt.BoltException;
import bolt.packet.DataPacket;
import bolt.packet.DeliveryType;
import bolt.packet.PacketUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Created by omahoc9 on 3/1/16.
 */
public class PacketCodec<T> implements Codec<T, List<DataPacket>>
{

    // TODO consider changing 1400 to a variable MTU
    private final int maxPacketSize = BoltEndPoint.DATAGRAM_SIZE - DataPacket.MAX_HEADER_SIZE;

    private final ObjectCodec<T> objectCodec;

    private DeliveryType deliveryType;

    public PacketCodec(final ObjectCodec<T> objectCodec)
    {
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
    public T decode(final List<DataPacket> data)
    {
        // TODO this method needs testing (include performance testing).
        final int byteCount = data.stream().map(d -> d.getData().length).reduce(0, (acc, x) -> acc + x);
        final byte[] bytes = new byte[byteCount];
        data.stream().map(DataPacket::getData).reduce(0, (acc, x) -> {
            System.arraycopy(x, 0, bytes, acc, x.length);
            return acc + x.length;
        }, (a, b) -> a + b);

        return objectCodec.decode(bytes);
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
        final int chunkCount = (int) Math.ceil(bytes.length / (double)maxPacketSize);
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
