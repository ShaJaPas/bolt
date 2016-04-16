package io.lyracommunity.bolt.sender;

import io.lyracommunity.bolt.util.SeqNum;

import java.util.concurrent.ConcurrentSkipListSet;

/**
 * The sender's loss list is used to store the sequence numbers of
 * the lost packets fed back by the receiver through NAK packets or
 * inserted in a timeout event. The numbers are stored in increasing order.
 */
class SenderLossList {

    private final ConcurrentSkipListSet<Integer> backingList;

    /**
     * Create a new sender lost list.
     */
    SenderLossList() {
        backingList = new ConcurrentSkipListSet<>(this::compareLosses);
    }

    void insert(final Integer obj) {
        backingList.add(obj);
    }

    void remove(final Integer obj) {
        backingList.remove(obj);
    }

    /**
     * Retrieves the loss list entry with the lowest sequence number, or null if loss list is empty.
     */
    Integer getFirstEntry() {
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

    private int compareLosses(final int o1, final int o2) {
        return SeqNum.compare16(o1, o2);
    }

}
