package bolt.sender;

import java.util.LinkedList;

/**
 * The sender's loss list is used to store the sequence numbers of
 * the lost packets fed back by the receiver through NAK packets or
 * inserted in a timeout event. The numbers are stored in increasing order.
 */
public class SenderLossList {

    private final LinkedList<Long> backingList;

    /**
     * Create a new sender lost list.
     */
    public SenderLossList() {
        backingList = new LinkedList<>();
    }

    public void insert(final Long obj) {
        synchronized (backingList) {
            for (int i = 0; i < backingList.size(); i++) {
                final Long entry = backingList.get(i);
                if (obj < entry) {
                    backingList.add(i, obj);
                    return;
                }
                else if (obj.equals(entry)) return;
            }
            backingList.add(obj);
        }
    }

    public void remove(Long obj) {
        synchronized (backingList) {
            backingList.remove(obj);
        }
    }

    /**
     * Retrieves the loss list entry with the lowest sequence number, or null if loss list is empty.
     */
    public Long getFirstEntry() {
        synchronized (backingList) {
            return backingList.poll();
        }
    }

    public boolean isEmpty() {
        return backingList.isEmpty();
    }

    public int size() {
        return backingList.size();
    }

    public String toString() {
        synchronized (backingList) {
            return backingList.toString();
        }
    }

}
