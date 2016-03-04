package bolt.xcoder;

import bolt.packets.DataPacket;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Created by omahoc9 on 3/1/16.
 */
public class PackageXCoder<T> implements XCoder<T, Collection<DataPacket>>
{

    private final int maxPacketSize = 1400;

    private final ObjectXCoder<T> objectXCoder;

    private final boolean reliable;

    public PackageXCoder(final ObjectXCoder<T> objectXCoder)
    {
        this(objectXCoder, true);
    }

    public PackageXCoder(final ObjectXCoder<T> objectXCoder, final boolean reliable) {
        Objects.requireNonNull(objectXCoder);
        this.objectXCoder = objectXCoder;
        this.reliable = reliable;
    }

    /**
     * Decodes a packet back into its original object.
     *
     * @param data the data packet to decode.
     * @return decoded object, or null if packet was a chunk of a yet incomplete message.
     */
    @Override
    public T decode(final Collection<DataPacket> data)
    {
        // TODO this method needs testing
        final int byteCount = data.stream().map(d -> d.getData().length).reduce(0, (acc, x) -> acc + x);
        final byte[] bytes = new byte[byteCount];
        data.stream().map(DataPacket::getData).reduce(0, (acc, x) -> {
            System.arraycopy(x, 0, bytes, acc, x.length);
            return acc + x.length;
        }, (a, b) -> a + b);

        return objectXCoder.decode(bytes);
    }

    /**
     * Encodes an object into a series of data packets.
     *
     * @param object the object to encode.
     * @return a collection of data packets.
     */
    @Override
    public Collection<DataPacket> encode(final T object) {
        final byte[] bytes = objectXCoder.encode(object);
        final boolean message = isMessage(bytes);
        final int chunkCount = (int) Math.ceil(bytes.length / (double)maxPacketSize);

        final List<DataPacket> dataPackets = new ArrayList<>();
        for (int i = 0; i < chunkCount; i++) {
            final int byteOffset = i * maxPacketSize;
            final int packetSize = Math.min(maxPacketSize, bytes.length - byteOffset);
            final byte[] packetData = new byte[packetSize];
            System.arraycopy(bytes, byteOffset, packetData, 0, packetSize);

            final DataPacket packet = new DataPacket();
            packet.setData(packetData);
            packet.setReliable(reliable);
            packet.setClassID(objectXCoder.getClassId());
            packet.setMessage(message);
            if (message) {
                packet.setMessageChunkNumber(i);
                packet.setFinalMessageChunk(i == (chunkCount - 1));
            }
            dataPackets.add(packet);
        }
        return dataPackets;
    }

    protected final boolean isMessage(final byte[] packetData) {
        return reliable && (packetData.length > maxPacketSize);
    }

    public void setClassId(final int classId) {
        objectXCoder.setClassId(classId);
    }

}
