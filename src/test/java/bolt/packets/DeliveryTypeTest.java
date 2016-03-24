package bolt.packets;

import org.junit.Test;

import java.util.stream.Stream;

import static org.junit.Assert.*;

/**
 * Created by omahoc9 on 3/24/16.
 */
public class DeliveryTypeTest
{

    @Test
    public void testUniqueDeliveryIds() {
        final long idCount = DeliveryType.values().length;
        final long distinctIdCount = Stream.of(DeliveryType.values()).map(DeliveryType::getId).distinct().count();
        assertEquals(idCount, distinctIdCount);
    }

}