package bolt;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * Created by keen on 15/03/16.
 */
public class TestReliability extends BoltTestBase {


    private final AtomicInteger deliveryCount = new AtomicInteger(0);
    private final Set<Throwable> errors = new HashSet<>();

    @Test
    public void testUnreliableWithPacketLoss() throws Throwable {
        final float packetLoss = 0.1f;
        final int sendCount = 50;
        final int expectedDeliveryCount = (int) Math.ceil(sendCount * (1f - packetLoss));
        final BoltServer server = runServer(byte[].class, x -> deliveryCount.incrementAndGet(), errors::add);
        server.config().setPacketLoss(packetLoss);

        runClient(server.getPort(),
                c -> {
                    c.getxCoderRepository().getXCoder(byte[].class).setReliable(false);
                    // Send unreliable
                    for (int i = 0; i < sendCount; i++) c.send(getRandomData(1000));
                    c.flush();
                },
                errors::add);

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

}
