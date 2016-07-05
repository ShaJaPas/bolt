package io.lyracommunity.bolt.codec;

import io.lyracommunity.bolt.packet.DataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by omahoc9 on 3/1/16.
 */
class CodecChain<T> {

    private static final Logger LOG = LoggerFactory.getLogger(CodecChain.class);

    private final ObjectSpliterator<T> spliterator;
    private final PacketCodec<T>       packageXCoder;

    private CodecChain(final ObjectSpliterator<T> spliterator, final PacketCodec<T> packageXCoder) {
        Objects.requireNonNull(packageXCoder);
        this.spliterator = spliterator;
        this.packageXCoder = packageXCoder;
    }

    static CodecChain rawBytePackageChain() {
        ObjectCodec<byte[]> byteXCoder = new ObjectCodec<byte[]>() {
            @Override
            public byte[] decode(byte[] data) {
                return data;
            }

            @Override
            public byte[] encode(byte[] object) {
                return object;
            }
        };
        return new CodecChain<>(null, new PacketCodec<>(byteXCoder));
    }

    public static <T> CodecChain<T> of(final PacketCodec<T> PackageXCoder) {
        return new CodecChain<>(null, PackageXCoder);
    }

    public T decode(final List<DataPacket> readyForDecode) {
        return packageXCoder.decode(readyForDecode);
    }

    public List<DataPacket> encode(final T object, final MessageAssembleBuffer assembleBuffer) {
        if (spliterator == null) {
            return encodeObject(object, assembleBuffer);
        }
        else {
            final Collection<T> split = spliterator.split(object);
            return split.stream()
                    .flatMap(t -> encodeObject(t, assembleBuffer).stream())
                    .collect(Collectors.toList());
        }
    }

    private List<DataPacket> encodeObject(final T object, final MessageAssembleBuffer assembleBuffer) {
        final List<DataPacket> packets = packageXCoder.encode(object);
        final boolean isMessage = packets.get(0).isMessage();
        if (isMessage) {
            final int messageId = assembleBuffer.nextMessageId();
            for (final DataPacket packet : packets) packet.setMessageId(messageId);
            LOG.info("Sending message {} with {} chunks.", messageId, packets.size());
        }
        return packets;
    }

    void setClassId(final int classId) {
        packageXCoder.setClassId(classId);
    }


}
