package bolt.receiver;

import bolt.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
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
public class ReceiverLossList {

    private final PriorityBlockingQueue<ReceiverLossListEntry> backingList;

    public ReceiverLossList() {
        backingList = new PriorityBlockingQueue<>(32);
    }

    public void insert(ReceiverLossListEntry entry) {
        /*
        TODO is synchronized necessary?
        Or use ConcurrentSet as complimentary to prevent duplicates?
        Or use ConcurrentSkipListSet?
         */
        synchronized (backingList) {
            if (!backingList.contains(entry)) {
                backingList.add(entry);
            }
        }
    }

    public void remove(int seqNo) {
        backingList.remove(new ReceiverLossListEntry(seqNo));
    }

    public boolean contains(ReceiverLossListEntry obj) {
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
    public ReceiverLossListEntry getFirstEntry() {
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
    public List<Integer> getFilteredSequenceNumbers(final long RTT, final boolean doFeedback) {
        final List<Integer> result = new ArrayList<>();
        final ReceiverLossListEntry[] sorted = backingList.toArray(new ReceiverLossListEntry[0]);
        Arrays.sort(sorted);
        for (ReceiverLossListEntry e : sorted) {
            if ((Util.getCurrentTime() - e.getLastFeedbackTime()) > e.getK() * RTT) {
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
