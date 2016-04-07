package io.lyracommunity.bolt;

import io.lyracommunity.bolt.codec.CodecRepository;
import io.lyracommunity.bolt.codec.MessageAssembleBuffer;
import io.lyracommunity.bolt.event.ReceiveObject;
import io.lyracommunity.bolt.packet.DataPacket;
import io.lyracommunity.bolt.packet.Destination;
import io.lyracommunity.bolt.session.ClientSession;
import io.lyracommunity.bolt.statistic.BoltStatistics;
import io.lyracommunity.bolt.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class BoltClient implements Client {

    private static final Logger LOG = LoggerFactory.getLogger(BoltClient.class);

    private final BoltEndPoint    clientEndpoint;
    private final CodecRepository codecs;
    private final Config          config;
    private       ClientSession   clientSession;

    public BoltClient(final InetAddress address, final int localPort) throws SocketException, UnknownHostException, IOException {
        this(new Config(address, localPort));
    }

    public BoltClient(final Config config) throws IOException {
        this.config = config;
        this.codecs = CodecRepository.basic(new MessageAssembleBuffer());
        this.clientEndpoint = new BoltEndPoint(config);
        LOG.info("Created client endpoint on port {}", clientEndpoint.getLocalPort());
    }

    @Override
    public Observable<?> connect(final InetAddress address, final int port) {

        return Observable.create(subscriber -> {
            Thread.currentThread().setName("Bolt-Poller-Client" + Util.THREAD_INDEX.incrementAndGet());
            Subscription endpointAndSession = null;
            try {
                endpointAndSession = Observable.merge(startEndPoint(), startSession(address, port))
                        .subscribe(subscriber::onNext, subscriber::onError, subscriber::onCompleted);
                while (!subscriber.isUnsubscribed()) {
                    if (clientSession != null) {
                        final DataPacket packet = clientSession.pollReceiveBuffer(10, TimeUnit.MILLISECONDS);

                        if (packet != null) {
                            final Object decoded = codecs.decode(packet);
                            if (decoded != null) {
                                subscriber.onNext(new ReceiveObject<>(clientSession.getSocketID(), decoded));
                            }
                        }
                    }
                }
            }
            catch (final InterruptedException ex) {
                LOG.info("Client interrupted. {}", ex.getMessage());
            }
            catch (final Exception ex) {
                subscriber.onError(ex);
            }
            if (endpointAndSession != null) {
                clientEndpoint.stop(subscriber);
                endpointAndSession.unsubscribe();
            }
            subscriber.onCompleted();
        });
    }

    @Override
    public void send(final Object obj, final int destId) throws IOException {
        send(obj);
    }

    public void send(final Object obj) throws BoltException {
        final Collection<DataPacket> data = codecs.encode(obj);
        for (final DataPacket dp : data) {
            try {
                send(dp);
            }
            catch (IOException ex) {
                throw new BoltException(ex);
            }
        }
        LOG.debug("Completed sending object {}", obj);
    }

    public void sendBlocking(final Object obj) throws BoltException {
        send(obj);
        flush();
    }

    private Observable<?> startEndPoint() throws InterruptedException, IOException {
        return clientEndpoint.start();
    }

    /**
     * Establishes a connection to the given server.
     * Starts the sender thread.
     *
     * @param address address of remote host.
     * @param port    port of remote host.
     * @throws UnknownHostException
     */
    private Observable<?> startSession(final InetAddress address, final int port) throws InterruptedException, IOException {
        final Destination destination = new Destination(address, port);
        // Create client session
        clientSession = new ClientSession(clientEndpoint, destination);
        clientEndpoint.addSession(clientSession.getSocketID(), clientSession);
        LOG.info("The BoltClient is connecting");
        return clientSession.connect();
    }

    /**
     * Sends the given data asynchronously.
     *
     * @param dataPacket the data and headers to send.
     * @throws IOException
     */
    public void send(final DataPacket dataPacket) throws IOException {
        clientSession.doWrite(dataPacket);
    }

    /**
     * Flush outstanding data, with the specified maximum waiting time.
     *
     * @throws BoltException
     */
    public void flush() throws BoltException {
        try {
            clientSession.flush();
        }
        catch (InterruptedException | IllegalStateException e) {
            throw new BoltException(e);
        }
    }

//    private void shutdown() {
//        if (clientSession.isReady() && clientSession.isActive()) {
//            clientEndpoint.stop();
//        }
//    }

    public BoltStatistics getStatistics() {
        return clientSession.getStatistics();
    }

    public CodecRepository codecs() {
        return codecs;
    }

    public Config config() {
        return config;
    }

}
