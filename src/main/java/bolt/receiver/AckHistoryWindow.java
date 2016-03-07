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
        final AckHistoryEntry obj = getEntry(ackNumber);
        return (obj == null) ? -1 : obj.getSentTime();
    }

    public AckHistoryEntry getEntry(long ackNumber) {
        for (Object obj : getArray()) {
            AckHistoryEntry e = (AckHistoryEntry) obj;
            if (e.getAckNumber() == ackNumber) {
                return e;
            }
        }
        return null;
    }

}
