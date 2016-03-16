package bolt.xcoder;

import bolt.packets.DataPacket;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by omahoc9 on 3/1/16.
 */
public class XCoderChain<T>
{

    private final ObjectSpliterator<T>  spliterator;
    private final PackageXCoder<T> packageXCoder;

    private XCoderChain(final ObjectSpliterator<T> spliterator, final PackageXCoder<T> packageXCoder)
    {
        Objects.requireNonNull(packageXCoder);
        this.spliterator = spliterator;
        this.packageXCoder = packageXCoder;
    }

    public static XCoderChain rawBytePackageChain() {
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
        return new XCoderChain<>(null, new PackageXCoder<>(byteXCoder));
    }

    private static <T> XCoderChain<T> of(final PackageXCoder<T> PackageXCoder) {
        return new XCoderChain<>(null, PackageXCoder);
    }

    public T decode(final Collection<DataPacket> readyForDecode)
    {
        return packageXCoder.decode(readyForDecode);
    }

    public Collection<DataPacket> encode(final T object)
    {
        if (spliterator == null) {
            return packageXCoder.encode(object);
        }
        else {
            //FIXME each split should have its own message id
            final Collection<T> split = spliterator.split(object);
            return split.stream()
                    .flatMap(t -> packageXCoder.encode(t).stream())
                    .collect(Collectors.toList());
        }
    }

    public void setClassId(final int classId) {
        packageXCoder.setClassId(classId);
    }

    public void setReliable(boolean reliable) {
        packageXCoder.setReliable(reliable);
    }

}
