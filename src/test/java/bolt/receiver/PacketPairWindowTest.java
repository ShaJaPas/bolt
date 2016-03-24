package bolt.receiver;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by omahoc9 on 3/24/16.
 */
public class PacketPairWindowTest {


    @Test
    public void testPacketPairWindow() {
        long[] values = {2, 4, 6};
        PacketPairWindow p = new PacketPairWindow(16);
        for (long value : values) p.add(value);

        assertEquals(4.0d, p.computeMedianTimeInterval(), 0.001d);

        long[] arrivaltimes = {12, 12, 12, 12};
        PacketPairWindow p1 = new PacketPairWindow(16);
        for (int i = 0; i < values.length; i++) {
            p1.add(arrivaltimes[i]);
        }
        assertEquals(12.0d, p1.computeMedianTimeInterval(), 0.001d);
    }

}