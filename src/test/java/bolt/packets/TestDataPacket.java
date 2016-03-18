package bolt.packets;

import org.junit.Test;

import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class TestDataPacket {


    private DataPacket createRandomPacket() {
        final Random r = new Random();
        final byte[] encodedData = new byte[100 + r.nextInt(1288)];
        r.nextBytes(encodedData);

        encodedData[0] = (byte) ((r.nextInt(5) << 4) | (r.nextInt(9))); // Delivery Type | First 4 seq no digits

        return new DataPacket(encodedData);
    }

    @Test
    public void testCopyIsSymmetric() {
        IntStream.range(0, 1000).parallel().forEach(__ -> {
            final DataPacket src = createRandomPacket();
            final DataPacket cpy = new DataPacket();
            cpy.copyFrom(src);

            assertEquals(src, cpy);
        });
    }

    @Test
    public void testMessageChunkNumber() {
        final Random r = new Random();
        IntStream.range(0, 1000).parallel().forEach(__ -> {
            int messageChunkNum = r.nextInt(PacketUtil.MAX_MESSAGE_CHUNK_NUM);
            final DataPacket src = createRandomPacket();
            src.setDelivery(DeliveryType.RELIABLE_ORDERED_MESSAGE);
            src.setMessageChunkNumber(messageChunkNum);
            final DataPacket cpy = new DataPacket(src.getEncoded());

            assertEquals(messageChunkNum, cpy.getMessageChunkNumber());
        });
    }

    @Test
    public void testDecodeAndEncodeAreSymmetric() {
        IntStream.range(0, 1000).parallel().forEach(__ -> {
            final DataPacket src = createRandomPacket();
            final DataPacket cpy = new DataPacket(src.getEncoded());

            assertEquals(src, cpy);
        });
    }

    @Test
    public void testSequenceNumber1() {
        DataPacket p = createRandomPacket();
        p.setPacketSequenceNumber(1);
        p.setData(new byte[0]);
        byte[] x = p.getEncoded();
        byte highest = x[0];
        // Check highest bit is "0" for DataPacket
        assertEquals(0, highest & 128);
        byte lowest = x[3];
        assertEquals(1, lowest);
    }

    @Test
    public void testEncoded() {
        DataPacket p = createRandomPacket();
        p.setDelivery(DeliveryType.RELIABLE_ORDERED);
        p.setPacketSequenceNumber(1);
        byte[] data = "test".getBytes();
        p.setData(data);
        byte[] encoded = p.getEncoded();
        byte[] encData = new byte[data.length];
        System.arraycopy(encoded, 8, encData, 0, data.length);
        String s = new String(encData);
        assertEquals("test", s);
        System.out.println("String s = " + s);
    }


    @Test
    public void testDecode1() {
        DataPacket testPacket1 = createRandomPacket();
        testPacket1.setDelivery(DeliveryType.RELIABLE_ORDERED);
        testPacket1.setPacketSequenceNumber(127);
        testPacket1.setDestinationID(1);
        byte[] data1 = "Hallo".getBytes();
        testPacket1.setData(data1);

        // Get the encoded data
        byte[] encodedData = testPacket1.getEncoded();

        int headerLength = 8;
        assertEquals(data1.length + headerLength, encodedData.length);

        byte[] payload = new byte[data1.length];
        System.arraycopy(encodedData, headerLength, payload, 0, data1.length);
        String s1 = new String(payload);
        assertEquals("Hallo", s1);

        // Create a new DataPacket from the encoded data
        DataPacket testPacket2 = new DataPacket(encodedData);

        assertEquals(127, testPacket2.getPacketSequenceNumber());
    }

    @Test
    public void testEncodeDecode1() {
        final DataPacket dp = createRandomPacket();
        dp.setPacketSequenceNumber(127);
        dp.setDelivery(DeliveryType.RELIABLE_ORDERED_MESSAGE);
        dp.setMessageId(35457);
        dp.setDestinationID(255);
        dp.setData("test".getBytes());

        byte[] encodedData1 = dp.getEncoded();

        final DataPacket dp2 = new DataPacket(encodedData1);
        assertEquals(true, dp2.isMessage());
        assertEquals(127, dp2.getPacketSequenceNumber());
        assertEquals(35457, dp2.getMessageId());
        assertEquals(255, dp2.getDestinationID());
        assertEquals("test", new String(dp2.getData()));
    }


}
