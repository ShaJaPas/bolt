package bolt.xcoder;

import bolt.packets.DataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by omahoc9 on 3/1/16.
 */
public class XCoderChain<T> {

    private static final Logger LOG = LoggerFactory.getLogger(XCoderChain.class);

    private final ObjectSpliterator<T> spliterator;
    private final MessageAssembleBuffer messageAssembleBuffer;
    private final PackageXCoder<T> packageXCoder;

    private XCoderChain(final MessageAssembleBuffer messageAssembleBuffer, final ObjectSpliterator<T> spliterator, final PackageXCoder<T> packageXCoder) {
        Objects.requireNonNull(messageAssembleBuffer);
        Objects.requireNonNull(packageXCoder);
        this.messageAssembleBuffer = messageAssembleBuffer;
        this.spliterator = spliterator;
        this.packageXCoder = packageXCoder;
    }

    public static XCoderChain rawBytePackageChain(final MessageAssembleBuffer assembler) {
        ObjectXCoder<byte[]> byteXCoder = new ObjectXCoder<byte[]>() {
            @Override
            public byte[] decode(byte[] data) {
                return data;
            }

            @Override
            public byte[] encode(byte[] object) {
                return object;
            }
        };
        return new XCoderChain<>(assembler, null, new PackageXCoder<>(byteXCoder));
    }

    public static <T> XCoderChain<T> of(final MessageAssembleBuffer assembler, final PackageXCoder<T> PackageXCoder) {
        return new XCoderChain<>(assembler, null, PackageXCoder);
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
