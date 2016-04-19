package io.lyracommunity.bolt.codec;

import io.lyracommunity.bolt.helper.PerfSupport;
import io.lyracommunity.bolt.helper.TestData;
import io.lyracommunity.bolt.helper.TestObjects;
import io.lyracommunity.bolt.packet.DataPacket;
import io.lyracommunity.bolt.packet.DeliveryType;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

/**
 * Created by keen on 18/04/16.
 */
public class CodecChainTest {



//    @Test
    public void performanceTest() {
        final int spins = 1_000_000;
        CodecRepository r = CodecRepository.basic();
        final MessageAssembleBuffer assembleBuffer = new MessageAssembleBuffer();
        TestObjects.registerAll(r);

        byte[] decodedRaw = TestData.getRandomData(1200);
        Object decodedRelOrd = TestObjects.reliableOrdered(300);
        final Collection<DataPacket> encodedRaw = r.encode(decodedRaw, assembleBuffer);
        final Collection<DataPacket> encodedRelOrd = r.encode(decodedRelOrd, assembleBuffer);

        Function<Collection<DataPacket>, Object> fun = dps -> {
            Object o = null;
            for(DataPacket dp : dps) o = r.decode(dp, assembleBuffer);
            return o;
        };

        PerfSupport.timed( () -> fun.apply(encodedRaw), spins);
        PerfSupport.timed( () -> fun.apply(encodedRelOrd), spins);
//        PerfSupport.timed( () -> fun.apply(createEncoded(true)), spins);
//        PerfSupport.timed( () -> fun.apply(createEncoded(false)), spins);
    }

    private Collection<DataPacket> createEncoded(boolean raw) {
        DataPacket dp = new DataPacket();
        dp.setClassID(raw ? 0 : 4);
        dp.setData(TestData.getRandomData(1200));
        dp.setDelivery(DeliveryType.RELIABLE_ORDERED);
        return Collections.singletonList(dp);
    }

}