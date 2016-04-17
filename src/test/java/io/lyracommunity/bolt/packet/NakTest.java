package io.lyracommunity.bolt.packet;

import io.lyracommunity.bolt.util.SeqNum;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

/**
 * Created by keen on 02/04/16.
 */
public class NakTest {


    @Test
    public void testOverflow() throws Throwable {
        final Random r = new Random();
        for (int i = 0; i < 100; i++) {
            final Nak nack = new Nak();

            final int start = SeqNum.MAX_SEQ_NUM_16_BIT - 1 - r.nextInt(1000);
            final int end = (start + 1 + r.nextInt(10_000)) % SeqNum.MAX_SEQ_NUM_16_BIT;
            nack.addLossRange(start, end);
            final byte[] encoded = nack.getEncoded();

            final Nak mirror = (Nak) PacketFactory.createPacket(encoded);

            assertEquals(nack, mirror);
        }
    }

    @Test
    public void measureComputedListPerformance() {
        final AtomicInteger result = new AtomicInteger(0);
        final Nak nak = new Nak();
        nak.addLossRange(600, 900);
        nak.addLossList(Arrays.asList(908, 988, 1655, 1788));
        nak.addLossRange(SeqNum.MAX_SEQ_NUM_16_BIT - 2000, 500);
        final int spins = 10_000;
        final Consumer<Runnable> timed = (r) -> {
            final long start = System.nanoTime();
            for (int i = 0; i < spins; i++) r.run();
            final long totalTimeInMillis = ((System.nanoTime() - start) / 1_000_000);
            System.out.println("TIME:  " + (totalTimeInMillis / ((float)spins)) + "ms");
            System.out.println("RESULT:  " + result.get());
        };
        timed.accept( () -> nak.computeExpandedLossList().forEach((x) -> {}));
    }


}
