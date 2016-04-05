package io.lyracommunity.bolt.receiver;

import io.lyracommunity.bolt.util.Util;

/**
 * Store the Sent Acknowledge packet number and the time it is sent out.
 */
public class AckHistoryEntry {

    private final long ackSequenceNumber;

    /** The sequence number prior to which all the packets have been received. */
    private final long ackNumber;

    /** Time when the Acknowledgement entry was sent. */
    private final long sentTime;

    public AckHistoryEntry(final long ackSequenceNumber, final long ackNumber, final long sentTime) {
        this.ackSequenceNumber = ackSequenceNumber;
        this.ackNumber = ackNumber;
        this.sentTime = sentTime;
    }


    public long getAckSequenceNumber() {
        return ackSequenceNumber;
    }

    public long getAckNumber() {
        return ackNumber;
    }

    public long getSentTime() {
        return sentTime;
    }

    /**
     * Get the age of this sent ack sequence number.
     *
     * @return the age.
     */
    public long getAge() {
        return Util.getCurrentTime() - sentTime;
    }


}
