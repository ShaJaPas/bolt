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
    // TODO consider memory leak cases

    @Test
    public void testAssembly_valid() throws Throwable {
        // Given
        final int chunks = 10;
        final List<DataPacket> messagePackets = createMessagePackets(chunks);

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
    public void testAssembly_withinSizeThreshold() throws Throwable {
        // TODO implement test
    }

    //    @Test(expected = Exception.class)
    public void testAssembly_aboveSizeThreshold() throws Throwable {
        // TODO implement test
    }

    private List<DataPacket> createMessagePackets(final int count) {
        return IntStream.range(0, count).boxed().map(i -> {
            DataPacket p = new DataPacket();
            p.setClassID(1);
            p.setData(TestData.getRandomData(1000));
            p.setMessageId(1);
            p.setMessageChunkNumber(i);
            p.setDelivery(DeliveryType.RELIABLE_ORDERED_MESSAGE);
            p.setFinalMessageChunk(i == count - 1);
            return p;
        })
                .collect(Collectors.toList());
    }

}
