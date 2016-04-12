package io.lyracommunity.bolt.util;

import io.lyracommunity.bolt.api.Config;
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

    private final PacketConsumer out;

    private final PacketConsumer onDrop;

    private final AtomicBoolean running = new AtomicBoolean();

    private DelayQueue<QosPacket> latencyQueue = new DelayQueue<>();

    private TokenBuffer<QosPacket> bandwidthFilter;

    private volatile long packetReceiveCount = 0;


    public NetworkQoSSimulationPipeline(final Config config, final PacketConsumer out, final PacketConsumer onDrop) {
        this(config, out, onDrop, new Random()::nextInt);
    }

    NetworkQoSSimulationPipeline(final Config config, final PacketConsumer out, final PacketConsumer onDrop,
                                 final IntUnaryOperator intRNG) {
        this.config = config;
        this.intRNG = intRNG;
        this.out = out;
        this.onDrop = onDrop;
        this.bandwidthFilter = TokenBuffer.perSecond(config::getSimulatedBandwidthInBytesPerSecond, 1024 * 8);
    }

    /**
     * Offer a packet into the latency buffer.
     *
     * @param packet packet to buffer.
     */
    public void offer(final Destination peer, final BoltPacket packet) {
        ++packetReceiveCount;

        if (isArtificialDrop()) {
            onDrop.accept(peer, packet);
        }
        else {
            if (isLatencyPipeRequired()) {
                pipeInToLatency(peer, packet);
            }
            else if (isBandwidthPipeRequired()) {
                pipeLatencyToBandwidth(peer, packet);
            }
            else {
                out.accept(peer, packet);
            }
        }
    }

    private void pipeInToLatency(final Destination peer, final BoltPacket packet) {
        ensurePipeRunning();
        // Compute jitter, latency and delay time in microseconds.
        final long jitter = (config.getSimulatedMaxJitter() <= 0)
                ? 0
                : intRNG.applyAsInt(config.getSimulatedMaxJitter()) * 1000L;
        final long latency = Math.max(0, config.getSimulatedLatency()) * 1000L;
        final long queueUntil = Util.getCurrentTime() + latency + jitter;
        latencyQueue.offer(new QosPacket(queueUntil, packet, peer));
    }

    private void pipeLatencyToBandwidth(final Destination peer, final BoltPacket packet) {
        ensurePipeRunning();

        if (!bandwidthFilter.offer(new QosPacket(packet, peer))) {
            onDrop.accept(peer, packet);
        }
    }

    private void pipeBandwidthToOut() {
        final QosPacket maybePacket = bandwidthFilter.poll();
        if (maybePacket != null) {
            out.accept(maybePacket.peer, maybePacket.packet);
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
                final QosPacket delayedPacket = latencyQueue.poll(5, TimeUnit.MILLISECONDS);
                if (delayedPacket != null) {
                    pipeLatencyToBandwidth(delayedPacket.peer, delayedPacket.packet);
                }

                pipeBandwidthToOut();
            }
            while (required && running.get());
        }
        catch (InterruptedException ex) {
            // End gracefully.
        }
        finally {
            running.set(false);

            latencyQueue.stream().forEach(delayed -> out.accept(delayed.peer, delayed.packet));
            latencyQueue.clear();
        }
    }

    private boolean isPipeRequired() {
        return (isLatencyPipeRequired() || isBandwidthPipeRequired());
    }

    private boolean isBandwidthPipeRequired() {
        return (config.getSimulatedBandwidth() > 0);
    }

    private boolean isLatencyPipeRequired() {
        return (config.getSimulatedLatency() > 0) || (config.getSimulatedMaxJitter() > 0);
    }

    private boolean isArtificialDrop() {
        final float dropRate = config.getPacketDropRate();
        return dropRate > 0 && (packetReceiveCount % dropRate < 1f);
    }

    public void close() {
        running.set(false);
    }

    public interface PacketConsumer extends BiConsumer<Destination, BoltPacket> {

    }

    private static class QosPacket implements Delayed, TokenBuffer.Token {

        private final long        timestamp;
        private final BoltPacket  packet;
        private final Destination peer;

        private QosPacket(final BoltPacket packet, final Destination peer) {
            this(Util.getCurrentTime(), packet, peer);
        }
        private QosPacket(final long timestamp, final BoltPacket packet, final Destination peer) {
            this.timestamp = timestamp;
            this.packet = packet;
            this.peer = peer;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(timestamp - Util.getCurrentTime(), TimeUnit.MICROSECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(timestamp, ((QosPacket) o).timestamp);
        }

        @Override
        public int getLength() {
            return packet.getLength();
        }
    }

}
