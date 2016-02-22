package bolt.receiver;

import bolt.util.Util;

/**
 * an entry in the {@link ReceiverLossList}
 */
public class ReceiverLossListEntry implements Comparable<ReceiverLossListEntry> {

    private final long sequenceNumber;
    private long lastFeedbackTime;
    private long k = 2;

    /**
     * constructor
     *
     * @param sequenceNumber
     */
    public ReceiverLossListEntry(long sequenceNumber) {
        if (sequenceNumber <= 0) {
            throw new IllegalArgumentException("Got sequence number " + sequenceNumber);
        }
        this.sequenceNumber = sequenceNumber;
        this.lastFeedbackTime = Util.getCurrentTime();
    }


    /**
     * call once when this seqNo is fed back in NAK
     */
    public void feedback() {
        k++;
        lastFeedbackTime = Util.getCurrentTime();
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * k is initialised as 2 and increased by 1 each time the number is fed back
     *
     * @return k the number of times that this seqNo has been feedback in NAK
     */
    public long getK() {
        return k;
    }

    public long getLastFeedbackTime() {
        return lastFeedbackTime;
    }

    /**
     * order by increasing sequence number
     */
    public int compareTo(ReceiverLossListEntry o) {
        return (int) (sequenceNumber - o.sequenceNumber);
    }


    public String toString() {
        return sequenceNumber + "[k=" + k + ",time=" + lastFeedbackTime + "]";
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (k ^ (k >>> 32));
        result = prime * result
                + (int) (sequenceNumber ^ (sequenceNumber >>> 32));
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ReceiverLossListEntry other = (ReceiverLossListEntry) obj;
        return (sequenceNumber == other.sequenceNumber);
    }

}
