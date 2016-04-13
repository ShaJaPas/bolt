package io.lyracommunity.bolt;

import io.lyracommunity.bolt.helper.Infra;
import io.lyracommunity.bolt.helper.TestObjects;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by omahoc9 on 3/24/16.
 */
public class NetworkJitterIT
{

    @Test
    public void testOrderingWithHighRandomJitter() throws Throwable {
        doTest(400, 100, TestObjects.reliableOrdered(100));
    }

    @Test
    public void testUnorderedPacketsWithJitter() throws Throwable {
        doTest(50, 100, TestObjects.reliableUnordered(100));
    }

    private void doTest(final int jitterInMillis, final int numPackets, final Object toSend) throws Throwable {

        Infra.Builder builder = Infra.Builder.withServerAndClients(1)
                .preconfigureServer(s -> s.config().setSimulatedMaxJitter(jitterInMillis))
                .onReadyClient((tc, evt) -> {
                    System.out.println("Connected, begin send.");
                    for (int i = 0; i < numPackets; i++) tc.client.send(toSend);
                })
                .setWaitCondition(inf -> inf.server().receivedOf(toSend.getClass()) < numPackets);

        try (Infra i = builder.build()) {
            final long millisTaken = i.start().awaitCompletion(1, TimeUnit.MINUTES);
            final int meanExpectedJitter = jitterInMillis / 2;

            assertEquals(numPackets, i.server().receivedOf(toSend.getClass()));
            assertTrue(meanExpectedJitter <= millisTaken);
        }
    }

}
