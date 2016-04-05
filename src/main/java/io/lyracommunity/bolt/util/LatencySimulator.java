package io.lyracommunity.bolt.util;

import io.lyracommunity.bolt.Config;
import io.lyracommunity.bolt.packet.BoltPacket;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.IntUnaryOperator;

/**
 * Created by keen on 05/04/16.
 */
public class LatencySimulator {

    private final Config config;

    private final IntUnaryOperator intRNG;

    private BlockingQueue<Entry> latencyQueue = new PriorityBlockingQueue<>();

    public LatencySimulator(final Config config) {
        this(config, new Random()::nextInt);
    }

    public LatencySimulator(final Config config, final IntUnaryOperator intRNG) {
        this.config = config;
        this.intRNG = intRNG;
    }

    /**
     * Offer a packet into the latency buffer.
     *
     * @param packet packet to buffer.
     */
    public void offer(final BoltPacket packet) {
        final int jitter = (config.getSimulatedMaxJitter() <= 0) ? 0 : intRNG.applyAsInt(config.getSimulatedMaxJitter());
        final int latency = Math.max(0, config.getSimulatedLatency());
        final long queueUntil = System.currentTimeMillis() + latency + jitter;
        latencyQueue.offer(new Entry(queueUntil, packet));
    }

    /**
     * Remove an item which has served its latency time, if any exist.
     *
     * @return the available packet, or null if none exist.
     */
    public BoltPacket poll() {
        final Entry earliest = latencyQueue.peek();
        return (earliest != null && earliest.timestamp <= System.currentTimeMillis())
                ? latencyQueue.poll().packet
                : null;
    }



    private static class Entry implements Comparable<Entry> {

        private final long timestamp;
        private final BoltPacket packet;

        public Entry(final long timestamp, final BoltPacket packet) {
            this.timestamp = timestamp;
            this.packet = packet;
        }

        @Override
        public int compareTo(final Entry o) {
            return Long.compare(timestamp, o.timestamp);
        }
    }

}
