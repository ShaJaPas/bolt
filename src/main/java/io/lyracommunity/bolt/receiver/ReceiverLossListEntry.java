package io.lyracommunity.bolt.receiver;

import io.lyracommunity.bolt.util.SeqNum;
import io.lyracommunity.bolt.util.Util;

import java.util.Objects;

/**
 * An entry in the {@link ReceiverLossList}.
 */
public class ReceiverLossListEntry implements Comparable<ReceiverLossListEntry> {

    private final int sequenceNumber;
    private long lastFeedbackTime;
    private long k = 2;

    /**
     * Instantiate a new object.
     *
     * @param sequenceNumber sequence number lost.
     */
    ReceiverLossListEntry(final int sequenceNumber) {
        if (sequenceNumber < 0) {
            throw new IllegalArgumentException("Got sequence number " + sequenceNumber);
        }
        this.sequenceNumber = sequenceNumber;
        this.lastFeedbackTime = Util.currentTimeMicros();
    }


    /**
     * Call once when this seqNo is fed back in NAK.
     */
    void feedback() {
        k++;
        lastFeedbackTime = Util.currentTimeMicros();
    }

    int getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * k is initialised as 2 and increased by 1 each time the number is fed back.
     *
     * @return k the number of times that this seqNo has been feedback in NAK.
     */
    public long getK() {
        return k;
    }

    public long getLastFeedbackTime() {
        return lastFeedbackTime;
    }

    /**
     * Order by increasing sequence number.
     */
    public int compareTo(final ReceiverLossListEntry o) {
        return SeqNum.compare16(sequenceNumber, o.sequenceNumber);
    }


    public String toString() {
        return sequenceNumber + "[k=" + k + ",time=" + lastFeedbackTime + "]";
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final ReceiverLossListEntry that = (ReceiverLossListEntry) o;
        return sequenceNumber == that.sequenceNumber;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sequenceNumber);
    }

}
