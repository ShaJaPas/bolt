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
public class CodecChain<T> {

    private static final Logger LOG = LoggerFactory.getLogger(CodecChain.class);

    private final ObjectSpliterator<T> spliterator;
    private final MessageAssembleBuffer messageAssembleBuffer;
    private final PacketCodec<T> packageXCoder;

    private CodecChain(final MessageAssembleBuffer messageAssembleBuffer, final ObjectSpliterator<T> spliterator, final PacketCodec<T> packageXCoder) {
        Objects.requireNonNull(messageAssembleBuffer);
        Objects.requireNonNull(packageXCoder);
        this.messageAssembleBuffer = messageAssembleBuffer;
        this.spliterator = spliterator;
        this.packageXCoder = packageXCoder;
    }

    public static CodecChain rawBytePackageChain(final MessageAssembleBuffer assembler) {
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
        return new CodecChain<>(assembler, null, new PacketCodec<>(byteXCoder));
    }

    public static <T> CodecChain<T> of(final MessageAssembleBuffer assembler, final PacketCodec<T> PackageXCoder) {
        return new CodecChain<>(assembler, null, PackageXCoder);
    }

    public T decode(final List<DataPacket> readyForDecode) {
        return packageXCoder.decode(readyForDecode);
    }

    public List<DataPacket> encode(final T object) {
        if (spliterator == null) {
            return encodeObject(object);
        }
        else {
            //FIXME each split should have its own message id
            final Collection<T> split = spliterator.split(object);
            return split.stream()
                    .flatMap(t -> encodeObject(t).stream())
                    .collect(Collectors.toList());
        }
    }

    private List<DataPacket> encodeObject(final T object) {
        final List<DataPacket> packets = packageXCoder.encode(object);
        final boolean isMessage = packets.get(0).isMessage();
        if (isMessage) {
            final int messageId = messageAssembleBuffer.nextMessageId();
            for (final DataPacket packet : packets) packet.setMessageId(messageId);
            LOG.info("Sending message {} with {} chunks.", messageId, packets.size());
        }
        return packets;
    }

    public void setClassId(final int classId) {
        packageXCoder.setClassId(classId);
    }


}
