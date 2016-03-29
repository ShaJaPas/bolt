package io.lyracommunity.bolt.receiver;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by omahoc9 on 3/24/16.
 */
public class AckHistoryWindowTest {

    @Test
    public void testAckHistoryWindow() {
        final AckHistoryEntry ackSeqNrA = new AckHistoryEntry(0, 1, 1263465050);
        final AckHistoryEntry ackSeqNrB = new AckHistoryEntry(1, 2, 1263465054);
        final AckHistoryEntry ackSeqNrC = new AckHistoryEntry(2, 3, 1263465058);

        final AckHistoryWindow recvWindow = new AckHistoryWindow(3);
        recvWindow.add(ackSeqNrA);
        recvWindow.add(ackSeqNrB);
        recvWindow.add(ackSeqNrC);
        final AckHistoryEntry entryA = recvWindow.getEntry(1);
        assertEquals(1263465050, entryA.getSentTime());
    }

}