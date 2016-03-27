package bolt;

import bolt.packets.DataPacket;
import bolt.packets.DeliveryType;
import bolt.packets.PacketUtil;
import bolt.xcoder.ObjectXCoder;
import bolt.xcoder.PackageXCoder;
import bolt.xcoder.XCoderRepository;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 * Created by omahoc9 on 3/24/16.
 */
public class XCoderTest {

    private XCoderRepository xCoderRepository;

    public void setUp(final DeliveryType deliveryType) throws Exception {
        xCoderRepository = XCoderRepository.create();
        xCoderRepository.register(XCodable.class, new PackageXCoder<>(new XCodableObjectXCoder(), deliveryType));
    }

    @Test
    public void testXCode() throws Throwable {
        // Given
        setUp(DeliveryType.RELIABLE_ORDERED_MESSAGE);

        // When
        final XCodable original = new XCodable(1, 2);
        final Collection<DataPacket> encoded = xCoderRepository.encode(original);
        final DataPacket single = encoded.stream().findFirst().orElse(null);
        final XCodable decoded = xCoderRepository.decode(single);

        // Then
        assertEquals(1, encoded.size());
        assertEquals(8, single.getData().length);
        assertEquals(original, decoded);
    }

    @Test(expected = NoSuchElementException.class)
    public void testEncode_ErrorNoEncoder() throws Throwable {
        // Given (don't register any XCoder)
        xCoderRepository = XCoderRepository.create();

        // When
        xCoderRepository.encode(new XCodable(1, 2));
    }

    @Test(expected = NoSuchElementException.class)
    public void testDecode_ErrorNoDecoder() throws Throwable {
        // Given (don't register any XCoder)
        xCoderRepository = XCoderRepository.create();
        final DataPacket dp = new DataPacket();
        dp.setClassID(1);
        dp.setData(new byte[8]);
        dp.setDelivery(DeliveryType.RELIABLE_ORDERED);

        // When
        xCoderRepository.decode(dp);
    }

    @Test(expected = BoltException.class)
    public void testEncode_ErrorPacketTooLargeForNonMessage() throws Throwable {
        // Given (register XCodable as non-message)
        setUp(DeliveryType.RELIABLE_ORDERED);

        // When
        xCoderRepository.encode(new XCodable(IntStream.range(0, 1000).boxed().collect(Collectors.toList())));
    }

    private static class XCodableObjectXCoder extends ObjectXCoder<XCodable> {

        @Override
        public XCodable decode(byte[] data) {
            final List<Integer> ints = new ArrayList<>();
            for (int i = 0; i < data.length; i += 4) ints.add(PacketUtil.decodeInt(data, i));
            return new XCodable(ints);
        }

        @Override
        public byte[] encode(XCodable object) {
            final byte[] encoded = new byte[object.ints.size() * 4];
            for (int i = 0; i < object.ints.size(); i++) {
                System.arraycopy(PacketUtil.encodeInt(object.ints.get(i)), 0, encoded, i * 4, 4);
            }
            return encoded;
        }
    }

    private static class XCodable {
        private final List<Integer> ints;

        private XCodable(Integer... ints) {
            this(Arrays.asList(ints));
        }

        private XCodable(List<Integer> ints) {
            this.ints = new ArrayList<>(ints);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final XCodable xCodable = (XCodable) o;
            return Objects.equals(ints, xCodable.ints);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ints);
        }
    }

}
