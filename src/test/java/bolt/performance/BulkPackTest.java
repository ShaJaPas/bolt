package bolt.performance;

import bolt.BoltServer;
import bolt.BoltTestBase;
import org.junit.Test;

import java.util.Random;

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
        final BoltServer server = runServer(byte[].class,
                x -> {
                    if (received.incrementAndGet() % 10_000 == 0) System.out.println("Received " + received.get());
                },
                errors::add);

        final byte[] data = new byte[SIZE];
        new Random().nextBytes(data);

        runClient(server.getPort(),
                c -> {
                    for (int i = 0; i < PACKET_COUNT; i++) {
                        c.send(data);
                        if (i % 10000 == 0) System.out.println(i);
                    }
                    c.flush();
                },
                errors::add);

        while (received.get() < PACKET_COUNT && errors.isEmpty()) Thread.sleep(10);
        if (!errors.isEmpty()) throw new RuntimeException(errors.iterator().next());

        assertEquals(PACKET_COUNT, received.get());
    }


}
