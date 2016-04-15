package io.lyracommunity.bolt.codec;

import io.lyracommunity.bolt.helper.TestData;
import io.lyracommunity.bolt.packet.DataPacket;
import io.lyracommunity.bolt.packet.DeliveryType;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * Created by omahoc9 on 3/24/16.
 */
public class MessageAssembleBufferTest {

    private MessageAssembleBuffer sut = new MessageAssembleBuffer();

    @Test
    public void testAssembly_valid() throws Throwable {
        // Given
        final int chunks = 10;
        final List<DataPacket> messagePackets = createMessagePackets(chunks, true);

        // When
        for (int i = 0; i < chunks - 1; i++) {
            assertTrue(sut.addChunk(messagePackets.get(i)).isEmpty());
        }
        final List<DataPacket> all = sut.addChunk(messagePackets.get(chunks - 1));

        // Then
        assertNotNull(all);
        assertEquals(10, all.size());
    }

    @Test
    public void testAssembly_invalidNoFinalChunk() throws Throwable {
        // Given
        final int chunks = 10;
        final List<DataPacket> messagePackets = createMessagePackets(chunks, false);

        // When
        for (int i = 0; i < chunks; i++) {
            assertTrue(sut.addChunk(messagePackets.get(i)).isEmpty());
        }
    }

    @Test
    public void testNextMessageId_InSequence() throws Throwable {
        // Given
        final int first = sut.nextMessageId();

        final int second = sut.nextMessageId();

        assertEquals(first, second - 1);
    }

    private List<DataPacket> createMessagePackets(final int count, final boolean markFinal) {
        final byte[] data = TestData.getRandomData(1000);
        return IntStream.range(0, count).boxed().map(i -> {
            DataPacket p = new DataPacket();
            p.setClassID(1);
            p.setData(data);
            p.setMessageId(1);
            p.setMessageChunkNumber(i);
            p.setDelivery(DeliveryType.RELIABLE_ORDERED_MESSAGE);
            if (markFinal) p.setFinalMessageChunk(i == count - 1);
            return p;
        })
                .collect(Collectors.toList());
    }

}
