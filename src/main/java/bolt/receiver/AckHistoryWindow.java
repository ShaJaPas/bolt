package bolt.receiver;

import bolt.util.CircularArray;

/**
 * ACK History Window: A circular array of each sent ACK and the time
 * it is sent out. The most recent value will overwrite the oldest
 * one if no more free space in the array.
 */
public class AckHistoryWindow extends CircularArray<AckHistoryEntry> {

    public AckHistoryWindow(int size) {
        super(size);
    }

    /**
     * @param ackNumber the ACK number.
     *
     * @return the time for the given seq no, or <code>-1</code> if not known.
     */
    public long getTime(long ackNumber) {
        for (AckHistoryEntry obj : circularArray) {
            if (obj.getAckNumber() == ackNumber) {
                return obj.getSentTime();
            }
        }
        return -1;
    }

    public AckHistoryEntry getEntry(long ackNumber) {
        for (AckHistoryEntry obj : circularArray) {
            if (obj.getAckNumber() == ackNumber) {
                return obj;
            }
        }
        return null;
    }

}
