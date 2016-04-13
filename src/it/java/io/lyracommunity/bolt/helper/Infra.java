package io.lyracommunity.bolt.helper;

import io.lyracommunity.bolt.BoltClient;
import io.lyracommunity.bolt.BoltServer;
import io.lyracommunity.bolt.event.ConnectionReady;
import io.lyracommunity.bolt.util.Util;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Created by keen on 09/04/16.
 */
public class Infra implements AutoCloseable {

    private final TestServer server;
    private final List<TestClient> clients;
    private final Predicate<Infra> waitCondition;
    private final AtomicLong totalTime = new AtomicLong();

    private Infra(TestServer server, List<TestClient> clients, Predicate<Infra> waitCondition) {
        this.server = server;
        this.clients = clients;
        this.waitCondition = waitCondition;
    }

    public Infra start() throws Exception {
        // Start servers/clients
        server.start();
        for (TestClient c : clients) c.start(server.server.getPort());
        return this;
    }

    public long awaitCompletion(final long time, final TimeUnit unit) throws Throwable {
        final long maxWaitMicros = unit.toMicros(time);
        final long startTime = Util.getCurrentTime();
        while (server.getErrors().isEmpty()
                && clients.stream().allMatch(c -> c.getErrors().isEmpty())
                && waitCondition.test(this)) {
            Thread.sleep(3);
            if (Util.getCurrentTime() - startTime > maxWaitMicros) throw new TimeoutException("Timed out");
        }
        final long readyTime = clients.stream().mapToLong(TestClient::getReadyTime).min().orElse(0);
        totalTime.set(System.currentTimeMillis() - readyTime);

        if (!server.getErrors().isEmpty()) throw server.getErrors().get(0);

        final Throwable clientEx = clients.stream().flatMap(c -> c.getErrors().stream()).findFirst().orElse(null);
        if (clientEx != null) throw clientEx;

        System.out.println("Connected to completion took " + totalTime.get() + " ms.");
        return totalTime.get();
    }

    @Override
    public void close() throws Exception {
        server.close();
        for (AutoCloseable c : clients) c.close();
    }

    public TestServer server() {
        return server;
    }

    public List<TestClient> clients() {
        return clients;
    }

    public static class Builder {

        private final int numClients;
        private Consumer<BoltServer> serverConfigurer;
        private Consumer<BoltClient> clientConfigurer;
        private BiConsumer<TestClient, Object> onEventClient;
        private BiConsumer<TestClient, ConnectionReady> onReadyClient;
        private BiConsumer<TestServer, Object> onEventServer;
        private BiConsumer<TestServer, ConnectionReady> onReadyServer;
        private Predicate<Infra> waitCondition;

        public static Builder withServerAndClients(final int numClients) {
            return new Builder(numClients);
        }

        private Builder(int numClients) {
            this.numClients = numClients;
        }

        public Builder preconfigureServer(Consumer<BoltServer> serverConfigurer) {
            this.serverConfigurer = serverConfigurer;
            return this;
        }

        public Builder preconfigureClients(Consumer<BoltClient> clientConfigurer) {
            this.clientConfigurer = clientConfigurer;
            return this;
        }

        public Builder onEventClient(BiConsumer<TestClient, Object> action) {
            this.onEventClient = action;
            return this;
        }

        public Builder onReadyClient(BiConsumer<TestClient, ConnectionReady> action) {
            this.onReadyClient = action;
            return this;
        }

        public Builder onEventServer(BiConsumer<TestServer, Object> action) {
            this.onEventServer = action;
            return this;
        }

        public Builder onReadyServer(BiConsumer<TestServer, ConnectionReady> action) {
            this.onReadyServer = action;
            return this;
        }

        public Builder setWaitCondition(Predicate<Infra> waitCondition) {
            this.waitCondition = waitCondition;
            return this;
        }

        public Infra build() throws Exception {

            final TestServer server = TestServer.runCustomServer(onEventServer, onReadyServer, serverConfigurer);

            final List<TestClient> clients = TestClient.runClients(numClients, server.server.getPort(),
                    onEventClient, onReadyClient, clientConfigurer);

            return new Infra(server, clients, waitCondition);
        }

    }

}
