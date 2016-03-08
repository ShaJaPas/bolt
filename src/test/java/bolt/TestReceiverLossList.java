package bolt;

import bolt.receiver.ReceiverLossList;
import bolt.receiver.ReceiverLossListEntry;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestReceiverLossList {

    @Test
    public void test1() {
        final ReceiverLossList l = new ReceiverLossList();
        final ReceiverLossListEntry e1 = new ReceiverLossListEntry(1);
        final ReceiverLossListEntry e2 = new ReceiverLossListEntry(3);
        final ReceiverLossListEntry e3 = new ReceiverLossListEntry(2);
        l.insert(e1);
        l.insert(e2);
        l.insert(e3);
        assertEquals(1, l.getFirstEntry().getSequenceNumber());
    }

}
