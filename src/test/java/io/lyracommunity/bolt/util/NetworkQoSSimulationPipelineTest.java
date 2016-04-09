package io.lyracommunity.bolt.util;

import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.packet.BoltPacket;
import io.lyracommunity.bolt.packet.DataPacket;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by keen on 05/04/16.
 */
public class NetworkQoSSimulationPipelineTest {

    private NetworkQoSSimulationPipeline subject;

    private BlockingQueue<BoltPacket> output;

    @Before
    public void setUp() throws Exception {
        setUp(0, 0, 0);
    }

    @Test
    public void test_noLatency_noJitter() throws Exception {
        final BoltPacket p = createPacket(0);

        subject.offer(null, p);

        assertEquals(p, output.poll());
    }

    @Test
    public void test_highLatency_noJitter() throws Exception {
        setUp(50, 0, 0);
        final BoltPacket p = createPacket(0);

        subject.offer(null, p);

        assertNull(output.poll());
        Thread.sleep(80);
        assertEquals(p, output.poll());
    }

    @Test
    public void test_noLatency_highJitter() throws Exception {
        final AtomicInteger n = new AtomicInteger();
        // Jitter will be: 25ms, 50ms, 75ms, 0ms
        IntUnaryOperator rand = (i) -> ((i / 4) * n.incrementAndGet()) % i;
        setUp(0, 100, 0, rand);

        // When
        IntStream.range(0, 4).forEach(i -> subject.offer(null, createPacket(i)));

        // Then
        assertEquals(3, output.poll(100, TimeUnit.MILLISECONDS).getPacketSeqNumber());
        assertNull(output.poll());
        Thread.sleep(80);  // Sleep for enough time to ensure rest are available.
        assertEquals(0, output.poll().getPacketSeqNumber());
        assertEquals(1, output.poll().getPacketSeqNumber());
        assertEquals(2, output.poll().getPacketSeqNumber());
    }

    @Test
    public void test_noLatency_fullPacketLoss() throws Exception {
        final int count = 4;
        setUp(0, 0, 1f);

        // When
        IntStream.range(0, count).forEach(i -> subject.offer(null, createPacket(i)));

        // Then
        for (int i = 0; i < count; i++) assertNull(output.poll());
    }

    @Test
    public void test_noLatency_halfPacketLoss() throws Exception {
        final int count = 4;
        setUp(0, 0, 0.5f);

        // When
        IntStream.range(0, count).forEach(i -> subject.offer(null, createPacket(i)));

        // Then
        assertEquals(0, output.poll().getPacketSeqNumber());
        assertEquals(2, output.poll().getPacketSeqNumber());
        assertNull(output.poll());
    }

    private BoltPacket createPacket(final int packetSeqNum) {
        final DataPacket p = new DataPacket();
        p.setPacketSeqNumber(packetSeqNum);
        return p;
    }

    private void setUp(final int latencyMillis, final int maxJitterMillis, float packetLossPercent) {
        setUp(latencyMillis, maxJitterMillis, packetLossPercent, new Random()::nextInt);
    }

    private void setUp(final int latencyMillis, final int maxJitterMillis, float packetLossPercent, final IntUnaryOperator intRandomSupplier) {
        final Config config = new Config(null, 0);
        config.setSimulatedLatency(latencyMillis);
        config.setSimulatedMaxJitter(maxJitterMillis);
        config.setPacketLoss(packetLossPercent);
        output = new ArrayBlockingQueue<>(32);
        subject = new NetworkQoSSimulationPipeline(config, (peer, pkt) -> output.offer(pkt),
                (peer, pkt) -> {}, intRandomSupplier);
    }

}