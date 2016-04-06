package io.lyracommunity.bolt.util;

import io.lyracommunity.bolt.Config;
import io.lyracommunity.bolt.packet.BoltPacket;
import io.lyracommunity.bolt.packet.Destination;

import java.util.Random;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.IntUnaryOperator;

/**
 * Created by keen on 05/04/16.
 */
public class NetworkQoSSimulationPipeline {

    private final Config config;

    private final IntUnaryOperator intRNG;

    private final BiConsumer<Destination, BoltPacket> pipeTo;

    private final AtomicBoolean running = new AtomicBoolean();

    private DelayQueue<DelayedPacket> latencyQueue = new DelayQueue<>();

    private volatile long packetReceiveCount = 0;


    public NetworkQoSSimulationPipeline(final Config config, final BiConsumer<Destination, BoltPacket> pipeTo) {
        this(config, pipeTo, new Random()::nextInt);
    }

    public NetworkQoSSimulationPipeline(final Config config, final BiConsumer<Destination, BoltPacket> pipeTo, final IntUnaryOperator intRNG) {
        this.config = config;
        this.intRNG = intRNG;
        this.pipeTo = pipeTo;
    }

    /**
     * Offer a packet into the latency buffer.
     *
     * @param packet packet to buffer.
     */
    public void offer(final BoltPacket packet, final Destination peer) {
        ++packetReceiveCount;

        // TODO consider re-linking drop with statistics
        if (!isArtificialDrop()) {
            if (isPipeRequired()) {
                ensurePipeRunning();
                final int jitter = (config.getSimulatedMaxJitter() <= 0) ? 0 : intRNG.applyAsInt(config.getSimulatedMaxJitter());
                final int latency = Math.max(0, config.getSimulatedLatency());
                final long queueUntil = System.currentTimeMillis() + latency + jitter;
                latencyQueue.offer(new DelayedPacket(queueUntil, packet, peer));
            }
            else {
                pipeTo.accept(peer, packet);
            }
        }
    }

    private void ensurePipeRunning() {
        // If network conditions are active and thread not active, start it now.
        if (running.compareAndSet(false, true)) {
            new Thread(this::runPipe).start();
        }
    }

    private void runPipe() {
        try {
            boolean required;
            do {
                required = isPipeRequired();
                final DelayedPacket delayedPacket = latencyQueue.poll(10, TimeUnit.MILLISECONDS);
                if (delayedPacket != null) {
                    pipeTo.accept(delayedPacket.peer, delayedPacket.packet);
                }
            }
            while (required);
        }
        catch (InterruptedException ex) {
            // End gracefully.
        }
        finally {
            running.set(false);
            latencyQueue.stream().forEach(delayed -> pipeTo.accept(delayed.peer, delayed.packet));
            latencyQueue.clear();
        }
    }

    private boolean isPipeRequired() {
        return (config.getSimulatedLatency() > 0) || (config.getSimulatedMaxJitter() > 0);
    }

    private boolean isArtificialDrop() {
        final float dropRate = config.getPacketDropRate();
        return dropRate > 0 && (packetReceiveCount % dropRate < 1f);
    }


    private static class DelayedPacket implements Delayed {

        private final long timestamp;
        private final BoltPacket packet;
        private final Destination peer;

        private DelayedPacket(final long timestamp, final BoltPacket packet, final Destination peer) {
            this.timestamp = timestamp;
            this.packet = packet;
            this.peer = peer;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(timestamp - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(timestamp, ((DelayedPacket) o).timestamp);
        }
    }

}
