package io.lyracommunity.bolt.util;

import io.lyracommunity.bolt.packet.BoltPacket;
import io.lyracommunity.bolt.packet.Destination;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Buffered alternative to Token Bucket.
 */
class TokenBuffer<T extends TokenBuffer.Token> {

    private static final long NANOS_IN_SECOND = 1_000_000_000L;

    /** How many tokens-worth the buffer can store before dropping items. */
    private final int bufferCapacity;

    /** Current number of tokens in the bucket. */
    private long tokenCount;

    /** Number of nanoseconds to replenish one token. */
    private LongSupplier replenishRate;

    /** Last replenishment time, in nanoseconds. */
    private long lastTime;

    /** Total token capacity. */
    private LongSupplier capacity;
    private BlockingQueue<T> buffer     = new LinkedBlockingQueue<>();
    private AtomicInteger    buffered   = new AtomicInteger();

    private TokenBuffer(final long tokenCount, final LongSupplier replenishRate, final LongSupplier capacity,
                        final int bufferCapacity) {
        this.lastTime = System.nanoTime();
        this.tokenCount = tokenCount;
        this.replenishRate = replenishRate;
        this.capacity = capacity;
        this.bufferCapacity = bufferCapacity;
    }

    static <S extends Token> TokenBuffer<S> perSecond(final LongSupplier tokensPerSec, final int bufferCapacity) {
        return perSecond(tokensPerSec.getAsLong(), tokensPerSec, bufferCapacity);
    }
    static <S extends Token> TokenBuffer<S> perSecond(final long initialCount, final LongSupplier tokensPerSec,
                                                      final int bufferCapacity) {
        final LongSupplier replenishRate = () -> {
            final long tps = tokensPerSec.getAsLong();
            return (tps == 0) ? 0 : NANOS_IN_SECOND / tps;
        };

        return new TokenBuffer<>(initialCount, replenishRate, tokensPerSec, bufferCapacity);
    }

    boolean offer(final T dp) {
        return tryAddToBuffer(dp);
    }

    T poll() {
        replenish();

        final T acquired = tryAcquireTokens(buffer.peek());
        if (acquired != null) {
            buffer.remove();
            buffered.addAndGet(-acquired.getLength());
        }
        return acquired;
    }

    void consumeAll(final Consumer<T> beforeRemove) {
        buffer.stream().forEach(beforeRemove);
        buffer.clear();
    }

    private T tryAcquireTokens(final T dp) {
        final boolean canAcquire = (dp != null) &&
                ((dp.getLength() <= tokenCount) || (capacity.getAsLong() == 0));
        if (canAcquire) {
            tokenCount -= dp.getLength();
        }
        return canAcquire ? dp : null;
    }

    private boolean tryAddToBuffer(final T dp) {
        final boolean canAdd = (dp.getLength() <= bufferCapacity - buffered.get());
        if (canAdd) {
            buffer.offer(dp);
            buffered.addAndGet(dp.getLength());
        }
        return canAdd;
    }

    private void replenish() {
        final long replenishRateLong = replenishRate.getAsLong();
        if (tokenCount >= capacity.getAsLong()) {
            lastTime = System.nanoTime();
        }
        else if (replenishRateLong > 0) {
            final long newTime = System.nanoTime();
            final long timePassed = newTime - lastTime;
            final long replenishCount = timePassed / replenishRateLong;
            if (replenishCount > 0) {
                final long leftover = timePassed % replenishRateLong;
                lastTime = newTime - leftover;
                tokenCount = Math.min(capacity.getAsLong(), tokenCount + replenishCount);
            }
        }
    }

    interface Token {
        int getLength();
    }
//
//
//    private class DelayItem<T> implements Delayed {
//
//        private final long        timestamp;
//        private final T  item;
//
//
//
//        @Override
//        public long getDelay(final TimeUnit unit) {
//            return unit.convert(timestamp - Util.getCurrentTime(), TimeUnit.MICROSECONDS);
//        }
//
//        @Override
//        public int compareTo(Delayed o) {
//            return Long.compare(timestamp, ((NetworkQoSSimulationPipeline.QosPacket) o).timestamp);
//        }
//
//        @Override
//        public int getLength() {
//            return packet.getLength();
//        }
//    }

}
