package bolt.sender;

import bolt.packets.DataPacket;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Holds a fixed number of {@link DataPacket} instances which are sent out.<br/>
 * <p>
 * It is assumed that a single thread (the producer) stores new data,
 * and another single thread (the consumer) reads/removes data.
 */
public class FlowWindow {

    private final DataPacket[] packets;

    private final int length;
    private final ReentrantLock lock;
    private volatile boolean isEmpty = true;
    private volatile boolean isFull = false;

    /**
     * Valid entries that can be read.
     */
    private volatile int validEntries = 0;

    /**
     * Index where the next data packet will be written to.
     */
    private volatile int writePos = 0;

    /**
     * One before the index where the next data packet will be read from.
     */
    private volatile int readPos = -1;
    private volatile int consumed = 0;
    private volatile int produced = 0;

    /**
     * @param flowWindowSize flow window size
     * @param chunkSize      data chunk size
     */
    public FlowWindow(final int flowWindowSize, final int chunkSize) {
        this.length = flowWindowSize + 1;
        packets = new DataPacket[length];
        for (int i = 0; i < packets.length; i++) {
            packets[i] = new DataPacket();
            packets[i].setData(new byte[chunkSize]);
        }
        lock = new ReentrantLock(true);
    }

    public boolean tryProduce(final DataPacket src) {
        // Do quick check before locking for performance.
        if (isFull) return false;
        lock.lock();
        try {
            if (isFull) return false;

            final DataPacket toProduce = packets[writePos];
            toProduce.copyFrom(src);

            if (toProduce.isFinalMessageChunk()) {
                System.out.println("FINAL PRODUCE");
            }

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

    public DataPacket consumeData() {
        // Do quick check before locking.
        if (isEmpty) return null;

        lock.lock();
        try {
            if (isEmpty) return null;

            DataPacket p = packets[++readPos];

            if (p.isFinalMessageChunk()) {
                System.out.println("FINAL CONSUME");
            }

            if (readPos == length - 1) readPos = -1;
            --validEntries;
            isEmpty = validEntries == 0;
            isFull = false;
            ++consumed;
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
    public boolean isFull() {
        return isFull;
    }


    public String toString() {
        return "FlowWindow size=" + length +
                " full=" + isFull + " empty=" + isEmpty +
                " readPos=" + readPos + " writePos=" + writePos +
                " consumed=" + consumed + " produced=" + produced;
    }

}
