package io.lyracommunity.bolt.helper;

import io.lyracommunity.bolt.codec.PacketCodec;
import io.lyracommunity.bolt.packet.DeliveryType;
import io.lyracommunity.bolt.packet.PacketUtil;
import io.lyracommunity.bolt.codec.ObjectCodec;
import io.lyracommunity.bolt.codec.CodecRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by omahoc9 on 3/25/16.
 */
public class TestObjects {

    public static void registerAll(final CodecRepository xcoding) {
        final PacketCodec<Finished> finishedXCoderChain = new PacketCodec<>(new ObjectCodec<Finished>() {
            @Override
            public Finished decode(byte[] data) {
                return new Finished();
            }

            @Override
            public byte[] encode(Finished object) {
                return new byte[8];
            }
        }, DeliveryType.RELIABLE_ORDERED);

        xcoding.register(UnreliableUnordered.class, createDataChain(DeliveryType.UNRELIABLE_UNORDERED, UnreliableUnordered::new));
        xcoding.register(ReliableUnordered.class, createDataChain(DeliveryType.RELIABLE_UNORDERED, ReliableUnordered::new));
        xcoding.register(ReliableUnorderedMessage.class, createDataChain(DeliveryType.RELIABLE_UNORDERED_MESSAGE, ReliableUnorderedMessage::new));
        xcoding.register(ReliableOrdered.class, createDataChain(DeliveryType.RELIABLE_ORDERED, ReliableOrdered::new));
        xcoding.register(ReliableOrderedMessage.class, createDataChain(DeliveryType.RELIABLE_ORDERED_MESSAGE, ReliableOrderedMessage::new));
        xcoding.register(Finished.class, finishedXCoderChain);
    }

    public static UnreliableUnordered unreliableUnordered(final int length) {
        return new UnreliableUnordered(length);
    }

    public static ReliableUnordered reliableUnordered(final int length) {
        return new ReliableUnordered(length);
    }

    public static ReliableUnorderedMessage reliableUnorderedMessage(final int length) {
        return new ReliableUnorderedMessage(length);
    }

    public static ReliableOrdered reliableOrdered(final int length) {
        return new ReliableOrdered(length);
    }

    public static ReliableOrderedMessage reliableOrderedMessage(final int length) {
        return new ReliableOrderedMessage(length);
    }

    public static Finished finished() {
        return new Finished();
    }

    private static <T extends BaseDataClass> PacketCodec<T> createDataChain(final DeliveryType deliveryType, final Function<List<Integer>, T> constructor) {
        final ObjectCodec<T> o = new ObjectCodec<T>() {
            @Override
            public T decode(byte[] data) {
                final List<Integer> ints = new ArrayList<>();
                for (int i = 0; i < data.length; i += 4) ints.add(PacketUtil.decodeInt(data, i));
                return constructor.apply(ints);
            }

            @Override
            public byte[] encode(T t) {
                final byte[] encoded = new byte[t.getData().size() * 4];
                for (int i = 0; i < t.getData().size(); i++) {
                    System.arraycopy(PacketUtil.encodeInt(t.getData().get(i)), 0, encoded, i * 4, 4);
                }
                return encoded;
            }
        };
        return new PacketCodec<>(o, deliveryType);
    }

    public static class BaseDataClass {
        private final List<Integer> data;

        BaseDataClass(final int length) {
            this.data = IntStream.range(0, length).boxed().collect(Collectors.toList());
        }

        BaseDataClass(final List<Integer> data) {
            this.data = new ArrayList<>(data);
        }

        public List<Integer> getData() {
            return data;
        }
    }

    public static class UnreliableUnordered extends BaseDataClass {

        private UnreliableUnordered(final int length) {
            super(length);
        }

        private UnreliableUnordered(final List<Integer> data) {
            super(data);
        }
    }

    public static class ReliableUnordered extends BaseDataClass {

        private ReliableUnordered(final int length) {
            super(length);
        }

        private ReliableUnordered(final List<Integer> data) {
            super(data);
        }
    }

    public static class ReliableUnorderedMessage extends BaseDataClass {

        private ReliableUnorderedMessage(final int length) {
            super(length);
        }

        private ReliableUnorderedMessage(final List<Integer> data) {
            super(data);
        }
    }

    public static class ReliableOrdered extends BaseDataClass {

        private ReliableOrdered(final int length) {
            super(length);
        }

        private ReliableOrdered(final List<Integer> data) {
            super(data);
        }
    }

    public static class ReliableOrderedMessage extends BaseDataClass {

        private ReliableOrderedMessage(final int length) {
            super(length);
        }

        private ReliableOrderedMessage(final List<Integer> data) {
            super(data);
        }
    }

    public static class Finished {

    }

}
