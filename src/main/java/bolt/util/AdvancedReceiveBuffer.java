package bolt.util;

import bolt.packets.DataPacket;

import java.util.*;
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
public class AdvancedReceiveBuffer
{

    private final Queue<DataPacket> buffer;

    /** Number of chunks. */
    private final AtomicInteger numValidChunks = new AtomicInteger(0);

    /** Lock and condition for poll() with timeout. */
    private final Condition notEmpty;
    private final ReentrantLock lock;

    /** The size of the buffer. */
    private final int size;

    private final DuplicateDetector duplicateDetector;

    /** The highest order sequence number already read by the application. */
    private int highestReadOrderNumber;

    public AdvancedReceiveBuffer(final int size, final int initialOrderNumber) {
        this.size = size;
        this.buffer = new PriorityBlockingQueue<>(size, new DataPacketPriorityComparator());
        this.lock = new ReentrantLock(false);
        this.notEmpty = lock.newCondition();
        this.highestReadOrderNumber = 0;
        this.duplicateDetector = new DuplicateDetector();
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
    public boolean offer(final DataPacket data) {
        if (numValidChunks.get() == size) {
            return false;
        }
        lock.lock();
        try {
            if (duplicateDetector.checkDuplicatePacket(data)) {
                return false;
            }
//            if (data.isOrdered()) { // FIXME potential to receive duplicate unordered packets?
//                final int seq = data.getOrderSeqNumber();
//                // If already have this chunk, discard it.
//                if (SequenceNumber.compare16(seq, highestReadOrderNumber) <= 0) {
//                    return true;
//                }
//            }
            // Else compute insert position.
            buffer.offer(data);
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
     * @return data chunk, or <tt>null</tt> if the specified waiting time
     * elapses before an element is available.
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
    // TODO this needs to be heavily test with many combinations (reliability|ordering)
    // TODO can be refactored for legibility
    public DataPacket poll() {
        if (numValidChunks.get() == 0) {
            return null;
        }
        final DataPacket r = buffer.peek();
        if (r != null) {
            // If packet is ordered, ensure that is it the next in the sequence to be read.
            if (r.isOrdered()) {
                final int thisSeq = r.getOrderSeqNumber();
                // TODO What about order number overflow?
                final int comparison = SequenceNumber.seqOffset16(highestReadOrderNumber, thisSeq);
                if (comparison == 1) {
                    highestReadOrderNumber = thisSeq;
                }
                else {
                    if (comparison <= 0) {
                        buffer.remove(r);
                        numValidChunks.decrementAndGet();
                    }
                    return null;
                }
            }

            // If unordered
            else {
                /*
                Circular array
                LinkedHashSet bounded
                Queue w/sorting
                BitSet
                 */

                // If packet sequence number has been read before:
                //      remove from buffer and return null
                // else:
                //      remove from buffer and mark read and return packet
            }
            numValidChunks.decrementAndGet();
            buffer.remove(r);
        }
        return r;
    }

    public int getNumChunks() {
        return numValidChunks.get();
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

            return SequenceNumber.compare(o1.getPacketSeqNumber(), o2.getPacketSeqNumber());
        }
    }

}
