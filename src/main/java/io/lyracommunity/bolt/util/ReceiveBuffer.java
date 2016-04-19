package io.lyracommunity.bolt.util;

import io.lyracommunity.bolt.packet.DataPacket;

import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The receive buffer stores data chunks to be read by the application.
 *
 * @author Cian O'Mahony
 */
public class ReceiveBuffer
{

    private final Queue<DataPacket> buffer;

    /** Number of chunks. */
    private final AtomicInteger numValidChunks = new AtomicInteger(0);

    /** Lock and condition for poll() with timeout. */
    private final Condition notEmpty;
    private final ReentrantLock lock;

    /** The size of the buffer. */
    private final int size;

    private static final int MAX_DUP_BUFFER = 100_000;
    private final DuplicateDetector duplicateDetector;

    /** The highest order sequence number already read by the application. */
    private int highestReadOrderNumber;

    public ReceiveBuffer(final int size) {
        this(size, 0);
    }

    ReceiveBuffer(final int size, final int initialOrderNumber) {
        this.size = size;
        this.buffer = new PriorityBlockingQueue<>(size, new DataPacketPriorityComparator());
        this.lock = new ReentrantLock(false);
        this.notEmpty = lock.newCondition();
        this.highestReadOrderNumber = initialOrderNumber;
        this.duplicateDetector = DuplicateDetector.ofSize(MAX_DUP_BUFFER);
    }

    /**
     * Offer a data packet into the buffer.
     * <p/>
     * This will only be accepted if the receive buffer is not already full.
     *
     * @param data the packet data to receive.
     * @return true if the the packet had already been received, or the buffer
     * successfully stored the packet; false if the buffer was too full to receive
     * the packet.
     */
    public OfferResult offer(final DataPacket data) {
        if (numValidChunks.get() == size) {
            return OfferResult.ERROR_BUFFER_FULL;
        }
        lock.lock();
        try {
            if (data.isOrdered()) {
                // If already have this chunk, discard it.
                final int orderSeqNo = data.getOrderSeqNumber();

                if (SeqNum.compare16(orderSeqNo, highestReadOrderNumber) <= 0) {
                    return OfferResult.OK_ACCEPTED;
                }
                // Prevent buffering packets that are too far ahead as the buffer
                // may become too full to accept the next in-order packet.
                else if (size <= SeqNum.seqOffset16(highestReadOrderNumber, orderSeqNo)) {
                    return OfferResult.ERROR_LOOKAHEAD;
                }
            }
            return insertAndSignal(data);
        }
        finally {
            lock.unlock();
        }
    }

    private OfferResult insertAndSignal(DataPacket data) {
        if (duplicateDetector.receivePacket(data)) {
            return OfferResult.ERROR_DUPLICATE;
        }
        buffer.offer(data);
        numValidChunks.incrementAndGet();
        notEmpty.signal();
        return OfferResult.OK_ACCEPTED;
    }

    /**
     * Return a data chunk, guaranteed to be in-order, waiting up to the
     * specified wait time if necessary for a chunk to become available.
     *
     * @param timeout how long to wait before giving up, in units of
     *                <tt>unit</tt>
     * @param unit    a <tt>TimeUnit</tt> determining how to interpret the
     *                <tt>timeout</tt> parameter
     * @return data chunk, or <tt>null</tt> if the specified waiting time
     * elapses before an element is available.
     * @throws InterruptedException if interrupted while waiting
     */
    public DataPacket poll(final int timeout, TimeUnit unit) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            long nanos = unit.toNanos(timeout);
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
        final DataPacket r = buffer.peek();
        if (r != null) {
            if (r.isOrdered()) {
                return removeOrdered(r);
            }
            else {
                return remove(r);
            }
        }
        return null;
    }

    /** If packet is ordered, ensure that is it the next in the sequence to be read. */
    private DataPacket removeOrdered(DataPacket r) {
        final int thisSeq = r.getOrderSeqNumber();

        final int comparison = SeqNum.seqOffset16(highestReadOrderNumber, thisSeq);
        if (comparison == 1) {
            highestReadOrderNumber = thisSeq;
            return remove(r);
        }
        else {
            if (comparison <= 0) {
                remove(r);
            }
            return null;
        }
    }

    private DataPacket remove(final DataPacket r) {
        buffer.remove(r);
        numValidChunks.decrementAndGet();
        return r;
    }

    private static class DataPacketPriorityComparator implements Comparator<DataPacket> {

        /**
         * Compares with the following priority:
         * <ol>
         *     <li>Unordered packets</li>
         *     <li>Ordered packets by seq number</li>
         * </ol>
         *
         * @param o1 Packet 1.
         * @param o2 Packet 2.
         * @return the ordering.
         */
        @Override
        public int compare(final DataPacket o1, final DataPacket o2)
        {
            if (o1.isOrdered() != o2.isOrdered()) return (o1.isOrdered() ? 1 : -1);

            return SeqNum.comparePacketSeqNum(o1.getPacketSeqNumber(), o2.getPacketSeqNumber());
        }
    }

    public enum OfferResult {
        OK_ACCEPTED("", true),
        ERROR_DUPLICATE("Duplicate packet", false),
        ERROR_LOOKAHEAD("Packet too far ahead to buffer", false),
        ERROR_BUFFER_FULL("Buffer is at capacity", false);

        public final String message;
        public final boolean success;

        OfferResult(String message, boolean success) {
            this.message = message;
            this.success = success;
        }
    }

}
