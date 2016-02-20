package bolt.packets;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class PacketUtil {

    public static byte[] encode(long value) {
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
     * encodes the specified address into 128 bit
     *
     * @param address - inet address
     */
    public static byte[] encode(InetAddress address) {
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
