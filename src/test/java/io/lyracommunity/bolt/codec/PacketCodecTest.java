package io.lyracommunity.bolt.codec;

import io.lyracommunity.bolt.api.BoltException;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.helper.TestObjects.BaseDataClass;
import io.lyracommunity.bolt.packet.DataPacket;
import io.lyracommunity.bolt.packet.DeliveryType;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static io.lyracommunity.bolt.helper.TestObjects.createPacketCodec;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by keen on 08/04/16.
 */
public class PacketCodecTest {

    private static final int DEFAULT_DATA_COUNT = Config.DEFAULT_DATAGRAM_SIZE - DataPacket.MAX_HEADER_SIZE;

    private PacketCodec<BaseDataClass> sut;

    private BaseDataClass o;

    private List<DataPacket> encoded;

    @Before
    public void setUp() throws Exception {
        setUp(DeliveryType.RELIABLE_ORDERED_MESSAGE);
    }

    private void setUp(final DeliveryType delivery) {
        sut = createPacketCodec(delivery, BaseDataClass::new);
        o = new BaseDataClass(DEFAULT_DATA_COUNT);
        encoded = sut.encode(o);
    }

    @Test
    public void decode() throws Exception {

        final BaseDataClass decoded = sut.decode(encoded);

        assertEquals(o, decoded);
    }

    @Test (expected = BoltException.class)
    public void encodeToMultiplePacketsWithNonMessageDelivery_NotAllowed() throws Exception {
        setUp(DeliveryType.RELIABLE_ORDERED);

        sut.encode(o);

        fail("Expected to fail due to multiple packets but no message delivery");
    }

    @Test
    public void encodeToMultiplePacketsAsMessage_Ok() throws Exception {
        final List<DataPacket> packets = sut.encode(o);

        assertEquals(4, packets.size());
    }

}