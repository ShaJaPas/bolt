package bolt.sender;

import java.util.concurrent.ConcurrentSkipListSet;

/**
 * The sender's loss list is used to store the sequence numbers of
 * the lost packets fed back by the receiver through NAK packets or
 * inserted in a timeout event. The numbers are stored in increasing order.
 */
public class SenderLossList {

    private final ConcurrentSkipListSet<Long> backingList;

    /**
     * Create a new sender lost list.
     */
    public SenderLossList() {
        backingList = new ConcurrentSkipListSet<>();
    }

    public void insert(final Long obj) {
        backingList.add(obj);
    }

    public void remove(Long obj) {
        backingList.remove(obj);
    }

    /**
     * Retrieves the loss list entry with the lowest sequence number, or null if loss list is empty.
     */
    public Long getFirstEntry() {
        return backingList.pollFirst();
    }

    public boolean isEmpty() {
        return backingList.isEmpty();
    }

    public int size() {
        return backingList.size();
    }

    public String toString() {
        return backingList.toString();
    }

}
