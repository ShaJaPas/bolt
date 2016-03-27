package io.lyracommunity.bolt;

import io.lyracommunity.bolt.receiver.ReceiverLossList;
import io.lyracommunity.bolt.receiver.ReceiverLossListEntry;
import org.junit.Test;

import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class ReceiverLossListTest {

    @Test
    public void test1() {
        final ReceiverLossList l = new ReceiverLossList();
        IntStream.of(1, 3, 2).forEach(i -> l.insert(new ReceiverLossListEntry(i)));
        assertEquals(1, l.getFirstEntry().getSequenceNumber());
    }

}
