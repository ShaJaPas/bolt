package bolt.performance;

import bolt.BoltClient;
import bolt.BoltServer;
import bolt.BoltTestBase;
import bolt.packets.DeliveryType;
import bolt.xcoder.*;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

/**
 * Created by keen on 26/02/16.
 */
public class BulkPackTest extends BoltTestBase
{

    private static final long PACKET_COUNT = 1_000_000;
    private static final int SIZE = 1388;


    @Test
    public void testBulkPackets() throws Exception {
        doTest(true, null, null);
    }

    @Test
    public void testBulkPackets_unreliable() throws Exception {
        final XCoderChain<byte[]> chain = XCoderChain.of(new PackageXCoder<>(new ObjectXCoder<byte[]>() {
            @Override
            public byte[] decode(byte[] data) {
                return data;
            }

            @Override
            public byte[] encode(byte[] object) {
                return object;
            }
        }, DeliveryType.UNRELIABLE_UNORDERED));

        doTest(false, s -> s.xCoderRepository().register(byte[].class, chain),
                c -> c.xCoderRepository().register(byte[].class, chain));
    }

    private void doTest(boolean waitForDelivery, final Consumer<BoltServer> serverInit, final Consumer<BoltClient> clientInit) throws Exception {
        final AtomicBoolean sendComplete = new AtomicBoolean(false);

        final BoltServer server = runServer(byte[].class,
                x -> {
                    if (received.incrementAndGet() % 10_000 == 0) System.out.println("Received " + received.get());
                },
                errors::add,
                serverInit);

        final byte[] data = new byte[SIZE];
        new Random().nextBytes(data);

        final BoltClient client = runClient(server.getPort(),
                c -> {
                    for (int i = 0; i < PACKET_COUNT; i++) {
                        c.send(data);
                        if (i % 10000 == 0) System.out.println(i);
                    }
                    c.flush();
                    sendComplete.set(true);
                },
                errors::add,
                clientInit);

        final Supplier<Boolean> done = waitForDelivery
                ? () -> received.get() < PACKET_COUNT
                : () -> !sendComplete.get();
        while (done.get() && errors.isEmpty()) Thread.sleep(10);
        if (!errors.isEmpty()) throw new RuntimeException(errors.iterator().next());

        System.out.println(client.getStatistics());

        assertEquals(PACKET_COUNT, received.get());
    }


}
