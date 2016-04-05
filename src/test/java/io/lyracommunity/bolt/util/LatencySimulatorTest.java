package io.lyracommunity.bolt.util;

import io.lyracommunity.bolt.Config;
import io.lyracommunity.bolt.packet.BoltPacket;
import io.lyracommunity.bolt.packet.DataPacket;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by keen on 05/04/16.
 */
public class LatencySimulatorTest {

    private LatencySimulator subject;

    @Before
    public void setUp() throws Exception {
        setUp(0, 0);
    }

    @Test
    public void test_noLatency_noJitter() throws Exception {
        final BoltPacket p = createPacket(0);

        subject.offer(p);

        assertEquals(p, subject.poll());
    }

    @Test
    public void test_highLatency_noJitter() throws Exception {
        setUp(50, 0);
        final BoltPacket p = createPacket(0);

        subject.offer(p);

        assertNull(subject.poll());
        Thread.sleep(80);
        assertEquals(p, subject.poll());
    }

    @Test
    public void test_noLatency_highJitter() throws Exception {
        final AtomicInteger n = new AtomicInteger();
        // Jitter will be: 25ms, 50ms, 75ms, 0ms
        IntUnaryOperator rand = (i) -> ((i / 4) * n.incrementAndGet()) % i;
        setUp(0, 100, rand);

        // When
        IntStream.range(0, 4).forEach(i -> subject.offer(createPacket(i)));

        // Then
        assertEquals(3, subject.poll().getPacketSeqNumber());
        assertNull(subject.poll());
        Thread.sleep(80);  // Sleep for enough time to ensure rest are available.
        assertEquals(0, subject.poll().getPacketSeqNumber());
        assertEquals(1, subject.poll().getPacketSeqNumber());
        assertEquals(2, subject.poll().getPacketSeqNumber());
    }

    private BoltPacket createPacket(final int packetSeqNum) {
        final DataPacket p = new DataPacket();
        p.setPacketSeqNumber(packetSeqNum);
        return p;
    }

    private void setUp(final int latencyMillis, final int maxJitterMillis) {
        setUp(latencyMillis, maxJitterMillis, new Random()::nextInt);
    }

    private void setUp(final int latencyMillis, final int maxJitterMillis, final IntUnaryOperator intRandomSupplier) {
        final Config config = new Config(null, 0);
        config.setSimulatedLatency(latencyMillis);
        config.setSimulatedMaxJitter(maxJitterMillis);
        subject = new LatencySimulator(config, intRandomSupplier);
    }


}