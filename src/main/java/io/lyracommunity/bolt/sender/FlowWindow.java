package io.lyracommunity.bolt.sender;

import io.lyracommunity.bolt.packet.DataPacket;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Holds a fixed number of {@link DataPacket} instances which are sent out.<br/>
 * <p>
 * It is assumed that a single thread (the producer) stores new data,
 * and another single thread (the consumer) reads/removes data.
 */
class FlowWindow {

    // TODO this can be a big source of memory when multiple sessions are open.
    private final DataPacket[] packets;

    private final int           length;
    private final int           chunkSize;
    private final ReentrantLock lock;
    private final Condition     notFull;
    private volatile boolean isEmpty      = true;
    private volatile boolean isFull       = false;
    /**
     * Valid entries that can be read.
     */
    private volatile int     validEntries = 0;

    /**
     * Index where the next data packet will be written to.
     */
    private volatile int writePos = 0;

    /**
     * One before the index where the next data packet will be read from.
     */
    private volatile int readPos  = -1;
    private volatile int consumed = 0;
    private volatile int produced = 0;

    /**
     * Initialize.
     *
     * @param preAllocateMemory whether to pre-allocate all memory for performance.
     * @param flowWindowSize    flow window size
     * @param chunkSize         data chunk size
     */
    FlowWindow(final boolean preAllocateMemory, final int flowWindowSize, final int chunkSize) {
        this.length = flowWindowSize + 1;
        this.chunkSize = chunkSize;
        this.packets = new DataPacket[length];
        if (preAllocateMemory) {
            for (int i = 0; i < packets.length; i++) {
                createDataPacket(i);
            }
        }
        this.lock = new ReentrantLock(true);
        this.notFull = lock.newCondition();
    }

    private DataPacket createDataPacket(final int index) {
        packets[index] = new DataPacket();
        packets[index].setData(new byte[chunkSize]);
        return packets[index];
    }

    private DataPacket getDataPacket(final int index) {
        final DataPacket dp = packets[index];
        return (dp != null) ? dp : createDataPacket(index);
    }

    boolean tryProduce(final DataPacket src, final int timeout, final TimeUnit unit) throws InterruptedException {
        // Do quick check before locking for performance.
        lock.lock();
        try {
            long nanos = unit.toNanos(timeout);
            while (isFull) {
                if (nanos <= 0) return false;
                nanos = notFull.awaitNanos(nanos);
            }

            final DataPacket toProduce = getDataPacket(writePos);
            toProduce.copyFrom(src);

            if (++writePos == length) writePos = 0;
            ++validEntries;
            ++produced;
            isEmpty = false;
            isFull = (validEntries == length - 1);
        }
        finally {
            lock.unlock();
        }
        return true;
    }

    DataPacket consumeData() {
        // Do quick check before locking.
        if (isEmpty) return null;

        lock.lock();
        try {
            if (isEmpty) return null;

            DataPacket p = packets[++readPos];

            if (readPos == length - 1) readPos = -1;
            --validEntries;
            isEmpty = validEntries == 0;
            isFull = false;
            ++consumed;
            notFull.signal();
            return p;
        }
        finally {
            lock.unlock();
        }
    }

    boolean isEmpty() {
        return isEmpty;
    }

    /**
     * Check if another entry can be added.
     *
     * @return true if window is full, otherwise false.
     */
    boolean isFull() {
        return isFull;
    }


    public String toString() {
        return "FlowWindow size=" + length +
                " full=" + isFull + " empty=" + isEmpty +
                " readPos=" + readPos + " writePos=" + writePos +
                " consumed=" + consumed + " produced=" + produced;
    }

}
