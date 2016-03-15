package bolt.util;

import bolt.packets.DataPacket;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The receive buffer stores data chunks to be read by the application.
 *
 * @author Cian O'Mahony
 */
public class ReceiveBuffer {

    private final DataPacket[] buffer;

    /** The lowest sequence number stored in this buffer. */
    private final int initialSequenceNumber;

    /** Number of chunks.. */
    private final AtomicInteger numValidChunks = new AtomicInteger(0);

    /** Lock and condition for poll() with timeout. */
    private final Condition notEmpty;
    private final ReentrantLock lock;

    /** The size of the buffer. */
    private final int size;

    /**
     * The head of the buffer: contains the next chunk to be read by the application, i.e. the one with the lowest sequence number.
     */
    private volatile int readPosition = 0;

    /** The highest sequence number already read by the application. */
    private int highestReadSequenceNumber;

    public ReceiveBuffer(final int size, final int initialSequenceNumber) {
        this.size = size;
        this.buffer = new DataPacket[size];
        this.initialSequenceNumber = initialSequenceNumber;
        lock = new ReentrantLock(false);
        notEmpty = lock.newCondition();
        highestReadSequenceNumber = SequenceNumber.decrement(initialSequenceNumber);
    }

    public boolean offer(final DataPacket data) {
        if (numValidChunks.get() == size) {
            return false;
        }
        lock.lock();
        try {
            final int seq = data.getPacketSequenceNumber();
            // If already have this chunk, discard it.
            if (SequenceNumber.compare(seq, highestReadSequenceNumber) <= 0) {
                return true;
            }
            // Else compute insert position.
            int offset = SequenceNumber.seqOffset(initialSequenceNumber, seq);
            int insert = offset % size;
            buffer[insert] = data;
            numValidChunks.incrementAndGet();
            notEmpty.signal();
            return true;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Return a data chunk, guaranteed to be in-order, waiting up to the
     * specified wait time if necessary for a chunk to become available.
     *
     * @param timeout how long to wait before giving up, in units of
     *                <tt>unit</tt>
     * @param unit    a <tt>TimeUnit</tt> determining how to interpret the
     *                <tt>timeout</tt> parameter
     * @return data chunk, or <tt>null</tt> if the
     * specified waiting time elapses before an element is available
     * @throws InterruptedException if interrupted while waiting
     */
    public DataPacket poll(int timeout, TimeUnit unit) throws InterruptedException {
        lock.lockInterruptibly();
        long nanos = unit.toNanos(timeout);

        try {
            for (; ; ) {
                if (numValidChunks.get() != 0) {
                    return poll();
                }
                if (nanos <= 0)
                    return null;
                try {
                    nanos = notEmpty.awaitNanos(nanos);
                }
                catch (InterruptedException ie) {
                    notEmpty.signal(); // propagate to non-interrupted thread
                    throw ie;
                }

            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Return a data chunk, guaranteed to be in-order.
     */
    public DataPacket poll() {
        if (numValidChunks.get() == 0) {
            return null;
        }
        final DataPacket r = buffer[readPosition];
        if (r != null) {
            int thisSeq = r.getPacketSequenceNumber();
            if (1 == SequenceNumber.seqOffset(highestReadSequenceNumber, thisSeq)) {
                increment();
                highestReadSequenceNumber = thisSeq;
            }
            else return null;
        }
        return r;
    }

    public int getSize() {
        return size;
    }

    public int getNumChunks() {
        return numValidChunks.get();
    }

    void increment() {
        buffer[readPosition] = null;
        readPosition++;
        if (readPosition == size) readPosition = 0;
        numValidChunks.decrementAndGet();
    }

    public boolean isEmpty() {
        return numValidChunks.get() == 0;
    }

}
