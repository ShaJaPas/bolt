package io.lyracommunity.bolt;

import io.lyracommunity.bolt.api.BoltBindException;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.api.event.ConnectionReady;
import io.lyracommunity.bolt.api.event.PeerDisconnected;
import io.lyracommunity.bolt.helper.Infra;
import io.lyracommunity.bolt.helper.TestObjects;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.lyracommunity.bolt.helper.TestSupport.sleepUnchecked;
import static org.junit.Assert.*;

/**
 * Created by omahoc9 on 3/24/16.
 */
public class ConnectivityIT {

    @Test
    public void testServerReactionToClientDisconnect() throws Throwable {
        Infra.Builder builder = Infra.Builder.withServerAndClients(1)
                .onReadyClient((tc, evt) -> tc.close())
                .setWaitCondition(inf -> inf.server().receivedOf(PeerDisconnected.class) < 1);

        try (Infra i = builder.build()) {
            i.start().awaitCompletion(1, TimeUnit.MINUTES);

            assertEquals(1, i.server().receivedOf(PeerDisconnected.class));
        }
    }

    @Test
    public void testServerReactionToMultiClientExpiry() throws Throwable {
        final int clientCount = 2;
        final AtomicInteger connected = new AtomicInteger(0);
        Infra.Builder builder = Infra.Builder.withServerAndClients(clientCount)
                .preconfigureServer(s -> s.config().setExpTimerInterval(10_000))
                .onReadyServer((ts, evt) -> {
                    if (connected.incrementAndGet() == clientCount) {
                        ts.server.config().setPacketLoss(1f);
                    }
                })
                .setWaitCondition(inf -> inf.server().receivedOf(PeerDisconnected.class) < clientCount);

        try (Infra i = builder.build()) {
            i.start().awaitCompletion(1, TimeUnit.MINUTES);

            assertEquals(clientCount, i.server().receivedOf(PeerDisconnected.class));
        }
    }

    @Test
    public void testClientAutoReconnectFromLapseInConnectivity() throws Throwable {
        final long disconnectTimeMillis = 200 + new Random().nextInt(300);
        final Object toSend = TestObjects.reliableOrdered(100);

        Infra.Builder builder = Infra.Builder.withServerAndClients(1)
                .onEventServer((ts, evt) -> {
                    ts.server.config().setPacketLoss(1f);
                    CompletableFuture.runAsync(() -> {
                        sleepUnchecked(disconnectTimeMillis);
                        ts.server.config().setPacketLoss(0f);
                    });
                })
                .onReadyClient((tc, evt) -> {
                    tc.client.sendBlocking(toSend);
                    sleepUnchecked(10);
                    tc.client.sendBlocking(toSend);
                })
                .setWaitCondition(inf -> inf.server().receivedOf(toSend.getClass()) < 2);

        try (Infra i = builder.build()) {
            final long millisTaken = i.start().awaitCompletion(1, TimeUnit.MINUTES);

            assertEquals(2, i.server().receivedOf(toSend.getClass()));
            assertTrue(millisTaken + " > " + disconnectTimeMillis, millisTaken > disconnectTimeMillis);
        }
    }

    @Test
    public void testClientTimesOut() throws Throwable {
        Infra.Builder builder = Infra.Builder.withServerAndClients(1)
                .preconfigureClients(c -> initExpConfig(c.config(), 6, 5000))
                .preconfigureServer(s -> initExpConfig(s.config(), 6, 5000))
                .onReadyServer((ts, evt) -> ts.server.config().setPacketLoss(1f))
                .setWaitCondition(inf ->
                        inf.server().receivedOf(PeerDisconnected.class) < 1
                                || inf.clients().get(0).receivedOf(PeerDisconnected.class) < 1);

        try (Infra i = builder.build()) {
            i.start().awaitCompletion(1, TimeUnit.MINUTES);

            assertEquals(1, i.server().receivedOf(PeerDisconnected.class));
            assertEquals(1, i.clients().get(0).receivedOf(PeerDisconnected.class));
        }
    }

    @Test(expected = TimeoutException.class)
    public void testKeepAliveOnIdle() throws Throwable {

        Infra.Builder builder = Infra.Builder.withServerAndClients(1)
                .preconfigureClients(c -> initExpConfig(c.config(), 6, 5000))
                .preconfigureServer(s -> initExpConfig(s.config(), 6, 5000))
                .setWaitCondition(inf ->
                        inf.server().receivedOf(PeerDisconnected.class) < 1
                                && inf.clients().get(0).receivedOf(PeerDisconnected.class) < 1);

        try (Infra i = builder.build()) {
            try {
                i.start().awaitCompletion(4, TimeUnit.SECONDS);
                fail("Did not expect a disconnect in either Server or Client");
            }
            catch (TimeoutException ex) {
                System.out.println("Caught timeout as expected");
                throw ex;
            }
        }
    }

    @Test
    public void testClientReactionToServerShutdown() throws Throwable {
        Infra.Builder builder = Infra.Builder.withServerAndClients(1)
                .onReadyServer((ts, evt) -> ts.close())
                .setWaitCondition(inf -> inf.clients().get(0).receivedOf(PeerDisconnected.class) < 1);

        try (Infra i = builder.build()) {
            i.start().awaitCompletion(1, TimeUnit.MINUTES);

            assertEquals(1, i.clients().get(0).receivedOf(PeerDisconnected.class));
        }
    }

    @Test
    public void testClientCannotConnect() throws Throwable {
        Infra.Builder builder = Infra.Builder.withServerAndClients(1)
                .preconfigureServer(s -> s.config().setPacketLoss(1f))
                .setWaitCondition(inf -> inf.clients().get(0).receivedOf(PeerDisconnected.class) < 1);

        try (Infra i = builder.build()) {
            try {
                i.start().awaitCompletion(1, TimeUnit.MINUTES);
                fail("Client connect should have failed with timeout");
            }
            catch (TimeoutException ex) {
                System.out.println("Caught expected timeout");
            }
        }
    }

    @Test(expected = BoltBindException.class)
    public void testServerCannotBind() throws Throwable {
        final Consumer<BoltServer> configureServer = s -> s.config().setLocalPort(9999);
        Infra.Builder builder = Infra.Builder.withServerAndClients(1)
                .preconfigureServer(configureServer)
                .setWaitCondition(inf -> inf.clients().get(0).receivedOf(ConnectionReady.class) < 1);

        try (Infra i1 = builder.build(); Infra i2 = builder.build()) {
            i1.start();

            Thread.sleep(100);

            i2.start().awaitCompletion(10, TimeUnit.SECONDS);
            fail("Expected port binding to fail for second server.");
        }
    }

    private void initExpConfig(final Config config, final int expLimit, final int expTimerInterval) {
        config.setExpLimit(expLimit);
        config.setExpTimerInterval(expTimerInterval);
    }

}
