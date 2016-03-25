package bolt;

import bolt.packets.DeliveryType;
import bolt.util.ClientUtil;
import bolt.util.ServerUtil;
import bolt.util.TestData;
import bolt.xcoder.ObjectXCoder;
import bolt.xcoder.PackageXCoder;
import bolt.xcoder.XCoderChain;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

/**
 * Created by keen on 15/03/16.
 */
public class ReliabilityTest {


    private final AtomicInteger deliveryCount = new AtomicInteger(0);
    private final Set<Throwable> errors = new HashSet<>();

    @Test
    public void testUnreliableWithPacketLoss() throws Throwable {
        final XCoderChain<String> stringXCoderChain = XCoderChain.of(new PackageXCoder<>(new ObjectXCoder<String>() {
            @Override
            public String decode(byte[] data)
            {
                return new String(data);
            }

            @Override
            public byte[] encode(String object)
            {
                return object.getBytes();
            }
        }, DeliveryType.UNRELIABLE_UNORDERED));

        final float packetLoss = 0.1f;
        final int sendCount = 50;
        final int expectedDeliveryCount = (int) Math.ceil(sendCount * (1f - packetLoss));
        final BoltServer server = ServerUtil.runServer(String.class, x -> deliveryCount.incrementAndGet(), errors::add);
        server.xCoderRepository().register(String.class, stringXCoderChain);
        server.config().setPacketLoss(packetLoss);

        final Consumer<BoltClient> initRegisterXCoder = (c) -> c.xCoderRepository().register(String.class, stringXCoderChain);

        ClientUtil.runClient(server.getPort(),
                c -> {
                    // Send unreliable
                    for (int i = 0; i < sendCount; i++) c.send(new String(TestData.getRandomData(500)));
                    c.flush();
                },
                errors::add, initRegisterXCoder);

        while (deliveryCount.get() < expectedDeliveryCount) {
            if (!errors.isEmpty()) throw errors.iterator().next();
            Thread.sleep(10);
        }

        assertEquals(expectedDeliveryCount, deliveryCount.get());
    }

    @Test
    public void testUnreliableAndReliableWithPacketLoss() {

//        final BoltServer server = runServer(byte[].class, x -> deliveryCount.incrementAndGet(), ex -> throw new RuntimeException(ex));

//        final BoltClient client = runClient(server.getPort(), x -> , ex -> throw new RuntimeException(ex));
    }

    @Test
    public void testReliableWithPacketLoss() throws Throwable {

    }


}
