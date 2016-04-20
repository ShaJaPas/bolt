package io.lyracommunity.bolt.receiver;

import io.lyracommunity.bolt.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Receiver's Loss List is a list of tuples whose values include:
 * the sequence numbers of detected lost data packets, the latest
 * feedback time of each tuple, and a parameter k that is the number
 * of times each one has been fed back in NAK. Values are stored in
 * the increasing order of packet sequence numbers.
 *
 * @see ReceiverLossListEntry
 */
class ReceiverLossList {

    private final PriorityBlockingQueue<ReceiverLossListEntry> backingList;

    ReceiverLossList() {
        backingList = new PriorityBlockingQueue<>(32);
    }

    void insert(ReceiverLossListEntry entry) {
        if (!contains(entry)) {
            backingList.add(entry);
        }
    }

    void remove(final int relSeqNo) {
        backingList.remove(new ReceiverLossListEntry(relSeqNo));
    }

    private boolean contains(final ReceiverLossListEntry obj) {
        return backingList.contains(obj);
    }

    public boolean isEmpty() {
        return backingList.isEmpty();
    }

    /**
     * Read (but NOT remove) the first entry in the loss list.
     *
     * @return the read entry.
     */
    ReceiverLossListEntry getFirstEntry() {
        return backingList.peek();
    }

    public int size() {
        return backingList.size();
    }

    /**
     * Return all sequence numbers whose last feedback time is larger than k * RTT.
     *
     * @param RTT        the current round trip time
     * @param doFeedback true if the k parameter should be increased and the time should
     *                   be reset (using {@link ReceiverLossListEntry#feedback()} )
     * @return the sequence numbers.
     */
    List<Integer> getFilteredSequenceNumbers(final long RTT, final boolean doFeedback) {
        final List<Integer> result = new ArrayList<>();
        final ReceiverLossListEntry[] sorted = backingList.toArray(new ReceiverLossListEntry[0]);
        Arrays.sort(sorted);
        for (ReceiverLossListEntry e : sorted) {
            if ((Util.currentTimeMicros() - e.getLastFeedbackTime()) > e.getK() * RTT) {
                result.add(e.getSequenceNumber());
                if (doFeedback) e.feedback();
            }
        }
        return result;
    }

    public String toString() {
        return backingList.toString();
    }


}
