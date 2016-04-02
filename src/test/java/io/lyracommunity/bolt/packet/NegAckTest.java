package io.lyracommunity.bolt.packet;

import io.lyracommunity.bolt.util.SeqNum;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Created by keen on 02/04/16.
 */
public class NegAckTest {


    @Test
    public void testOverflow() throws Throwable {
        final Random r = new Random();
        for (int i = 0; i < 100; i++) {
            final NegAck nack = new NegAck();

            final int start = SeqNum.MAX_SEQ_NUM_16_BIT - 1 - r.nextInt(1000);
            final int end = (start + 1 + r.nextInt(10_000)) % SeqNum.MAX_SEQ_NUM_16_BIT;
            nack.addLossInfo(start, end);
            final byte[] encoded = nack.getEncoded();

            final NegAck mirror = (NegAck) PacketFactory.createPacket(encoded);

            assertEquals(nack, mirror);
        }
    }


}
