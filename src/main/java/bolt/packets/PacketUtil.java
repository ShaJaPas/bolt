package bolt.packets;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class PacketUtil {

    public static final int MAX_MESSAGE_CHUNK_NUM = (int) Math.pow(2, 15) - 1;
    public static final int MAX_MESSAGE_ID = (int) Math.pow(2, 16) - 1;

    public static byte[] encode(long value) {
        byte m4 = (byte) (value >> 24);
        byte m3 = (byte) (value >> 16);
        byte m2 = (byte) (value >> 8);
        byte m1 = (byte) (value);
        return new byte[]{m4, m3, m2, m1};
    }

    public static byte[] encodeInt(int value) {
        byte m4 = (byte) (value >> 24);
        byte m3 = (byte) (value >> 16);
        byte m2 = (byte) (value >> 8);
        byte m1 = (byte) (value);
        return new byte[]{m4, m3, m2, m1};
    }


    public static byte[] encodeSetHighest(boolean highest, long value) {
        byte m4;
        if (highest) {
            m4 = (byte) (0x80 | value >> 24);
        } else {
            m4 = (byte) (0x7f & value >> 24);
        }
        byte m3 = (byte) (value >> 16);
        byte m2 = (byte) (value >> 8);
        byte m1 = (byte) (value);
        return new byte[]{m4, m3, m2, m1};
    }


    public static byte[] encodeControlPacketType(int type) {
        byte m4 = (byte) 0x80;

        byte m3 = (byte) type;
        return new byte[]{m4, m3, 0, 0};
    }

    public static boolean isBitSet(int data, int position) {
        return ((data >> position) & 1) == 1;
    }

    public static byte setBit(byte b, int bitIndex, boolean isSet) {
        return (byte) (isSet
                ? b | (1 << bitIndex)   // set bit to 1
                : b & ~(1 << bitIndex)); // set bit to 0
    }

    /**
     * @param value value to encode.
     * @param data byte array to encode into.
     * @param startBit first bit in data to set, as an offset from the right-most bit.
     * @param numBitsToSet number of bits to map.
     */
    public static byte[] encodeMapToBytes(int value, byte[] data, int startBit, int numBitsToSet) {
        for (int i = 0; i < numBitsToSet; i++) {
            final int bitIndex = startBit - i;
            final int byteIndex = (bitIndex / 8);
            final int bitInByteIndex = 7 - (bitIndex % 8);
            final boolean bitSet = isBitSet(value, i);
            data[byteIndex] = setBit(data[byteIndex], bitInByteIndex, bitSet);
        }
        return data;
    }

    public static int decodeInt(byte[] data, int start) {
        return (data[start] & 0xFF) << 24
                | (data[start + 1] & 0xFF) << 16
                | (data[start + 2] & 0xFF) << 8
                | (data[start + 3] & 0xFF);
    }

    public static int decodeInt(byte[] data, int start, int startBit, int endBit) {
        int decoded = decodeInt(data, start);
        decoded = decoded >> startBit;
        return decoded & ((int)Math.pow(2, endBit - startBit) - 1);
    }

    public static long decode(byte[] data, int start) {
        long result = (data[start] & 0xFF) << 24
                | (data[start + 1] & 0xFF) << 16
                | (data[start + 2] & 0xFF) << 8
                | (data[start + 3] & 0xFF);
        return result;
    }

    public static int decodeType(byte[] data, int start) {
        int result = data[start + 1] & 0xFF;
        return result;
    }

    /**
     * Encodes the specified address into 128 bit.
     *
     * @param address INet address.
     */
    public static byte[] encode(final InetAddress address) {
        byte[] res = new byte[16];
        byte[] add = address.getAddress();
        System.arraycopy(add, 0, res, 0, add.length);
        return res;
    }

    public static InetAddress decodeInetAddress(byte[] data, int start, boolean ipV6) throws UnknownHostException {
        byte[] add = ipV6 ? new byte[16] : new byte[4];
        System.arraycopy(data, start, add, 0, add.length);
        return InetAddress.getByAddress(add);
    }

}
