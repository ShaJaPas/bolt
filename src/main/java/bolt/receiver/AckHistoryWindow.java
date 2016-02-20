package bolt.receiver;

import bolt.util.CircularArray;

/**
 * a circular array of each sent Ack and the time it is sent out
 */
public class AckHistoryWindow extends CircularArray<AckHistoryEntry> {

    public AckHistoryWindow(int size) {
        super(size);
    }

    /**
     * return  the time for the given seq no, or <code>-1 </code> if not known
     *
     * @param ackNumber
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
