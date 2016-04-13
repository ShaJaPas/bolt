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

    private final AtomicBoolean latencyRunning = new AtomicBoolean();

    private final AtomicBoolean bandwidthRunning = new AtomicBoolean();

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
        this.bandwidthFilter = TokenBuffer.perSecond(config::getSimulatedBandwidthInBytesPerSecond, 1 << 16);
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
        ensureLatencyPipeRunning();
        // Compute jitter, latency and delay time in microseconds.
        final long jitter = (config.getSimulatedMaxJitter() <= 0)
                ? 0
                : intRNG.applyAsInt(config.getSimulatedMaxJitter()) * 1000L;
        final long latency = Math.max(0, config.getSimulatedLatency()) * 1000L;
        final long queueUntil = Util.getCurrentTime() + latency + jitter;
        latencyQueue.offer(new QosPacket(queueUntil, packet, peer));
    }

    private void pipeLatencyToBandwidth(final Destination peer, final BoltPacket packet) {
        ensureBandwidthPipeRunning();

        if (!bandwidthFilter.offer(new QosPacket(packet, peer))) {
            onDrop.accept(peer, packet);
        }
    }

    private boolean pipeBandwidthToOut() throws InterruptedException {
        // TODO inefficient constant loop. Find some way to optimize.
        final QosPacket maybePacket = bandwidthFilter.poll();
        final boolean packetReady = (maybePacket != null);
        if (packetReady) {
            out.accept(maybePacket.peer, maybePacket.packet);
        }
        return packetReady;
    }

    private void ensureLatencyPipeRunning() {
        if (latencyRunning.compareAndSet(false, true)) {
            new Thread(this::runLatencyPipe).start();
        }
    }

    private void ensureBandwidthPipeRunning() {
        if (bandwidthRunning.compareAndSet(false, true)) {
            new Thread(this::runBandwidthPipe).start();
        }
    }

    private void runBandwidthPipe() {
        try {
            boolean required;
            do {
                required = isBandwidthPipeRequired();

                pipeBandwidthToOut();
            }
            while (required && bandwidthRunning.get());
        }
        catch (InterruptedException ex) {
            // End gracefully.
        }
        finally {
            bandwidthRunning.set(false);
            bandwidthFilter.consumeAll(limited -> out.accept(limited.peer, limited.packet));
        }
    }

    private void runLatencyPipe() {
        try {
            boolean required;
            do {
                required = isLatencyPipeRequired();

                final QosPacket delayedPacket = latencyQueue.poll(20, TimeUnit.MILLISECONDS);
                if (delayedPacket != null) {
                    pipeLatencyToBandwidth(delayedPacket.peer, delayedPacket.packet);
                }
            }
            while (required && latencyRunning.get());
        }
        catch (InterruptedException ex) {
            // End gracefully.
        }
        finally {
            latencyRunning.set(false);
            latencyQueue.stream().forEach(delayed -> out.accept(delayed.peer, delayed.packet));
            latencyQueue.clear();
        }
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
        latencyRunning.set(false);
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
        public long getDelay(final TimeUnit unit) {
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
