package io.lyracommunity.bolt.helper;

import io.lyracommunity.bolt.codec.CodecRepository;
import io.lyracommunity.bolt.codec.ObjectCodec;
import io.lyracommunity.bolt.codec.PacketCodec;
import io.lyracommunity.bolt.packet.DeliveryType;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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

        xcoding.register(UnreliableUnordered.class, createPacketCodec(DeliveryType.UNRELIABLE_UNORDERED, UnreliableUnordered::new));
        xcoding.register(ReliableUnordered.class, createPacketCodec(DeliveryType.RELIABLE_UNORDERED, ReliableUnordered::new));
        xcoding.register(ReliableUnorderedMessage.class, createPacketCodec(DeliveryType.RELIABLE_UNORDERED_MESSAGE, ReliableUnorderedMessage::new));
        xcoding.register(ReliableOrdered.class, createPacketCodec(DeliveryType.RELIABLE_ORDERED, ReliableOrdered::new));
        xcoding.register(ReliableOrderedMessage.class, createPacketCodec(DeliveryType.RELIABLE_ORDERED_MESSAGE, ReliableOrderedMessage::new));
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

    public static <T extends BaseDataClass> PacketCodec<T> createPacketCodec(final DeliveryType deliveryType, final Function<List<Integer>, T> constructor) {
        final ObjectCodec<T> o = new ObjectCodec<T>() {
            @Override
            public T decode(byte[] data) {
                final List<Integer> ints = new ArrayList<>(data.length * 4);
                IntBuffer ib = ByteBuffer.wrap(data).asIntBuffer();
                while (ib.hasRemaining()) {
                    ints.add(ib.get());
                }
                return constructor.apply(ints);
            }

            @Override
            public byte[] encode(T t) {
                ByteBuffer ib = ByteBuffer.allocate(t.getData().size() * 4);
                for (Integer i : t.getData()) ib.putInt(i);
                return ib.array();
            }
        };
        return new PacketCodec<>(o, deliveryType);
    }

    public static void main(String[] args) {

    }

    private static final byte[] dd = TestData.getRandomData(1200);

    public static class BaseDataClass {
        private final List<Integer> data;

        public BaseDataClass(final int length) {
            this.data = IntStream.range(0, length).boxed().collect(Collectors.toList());
        }

        public BaseDataClass(final List<Integer> data) {
            this.data = data;
        }

        public List<Integer> getData() {
            return data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BaseDataClass that = (BaseDataClass) o;
            return Objects.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            return Objects.hash(data);
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
