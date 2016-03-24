package bolt.receiver;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by omahoc9 on 3/24/16.
 */
public class PacketHistoryWindowTest {

    @Test
    public void testPacketHistoryWindow() {

        PacketHistoryWindow packetHistoryWindow = new PacketHistoryWindow(16);
        long offset = 1000000;
        for (int i = 0; i < 28; i++) {
            packetHistoryWindow.add(offset + i * 5000L);
        }
        // Packets arrive every 5 ms, so packet arrival rate is 200/sec
        assertEquals(200, packetHistoryWindow.getPacketArrivalSpeed());
    }

}