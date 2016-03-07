package bolt.packets;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by keen on 07/03/16.
 */
public class PacketUtilTest {

    @Test
    public void testEncodeMapToBytes() throws Exception {
        byte[] actual = PacketUtil.encodeMapToBytes(3, new byte[4], 31, 4);

        assertArrayEquals(new byte[] {0, 0, 0, 3}, actual);
    }
}