package bolt;

import bolt.packets.DataPacket;
import bolt.packets.Destination;
import bolt.packets.Shutdown;
import bolt.receiver.RoutedData;
import bolt.statistic.BoltStatistics;
import bolt.xcoder.MessageAssembleBuffer;
import bolt.xcoder.XCoderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BoltClient implements Client {

    private static final Logger LOG = LoggerFactory.getLogger(BoltClient.class);

    private final BoltEndPoint clientEndpoint;
    private final XCoderRepository xCoderRepository;
    private ClientSession clientSession;
    private final Config config;

    public BoltClient(final InetAddress address, final int localPort) throws SocketException, UnknownHostException {
        this(new Config(address, localPort));
    }

    public BoltClient(final Config config) throws SocketException, UnknownHostException {
        this.config = config;
        this.xCoderRepository = XCoderRepository.create(new MessageAssembleBuffer());
        this.clientEndpoint = new BoltEndPoint(config);
        LOG.info("Created client endpoint on port " + clientEndpoint.getLocalPort());
    }

    @Override
    public Observable<?> connect(final InetAddress address, final int port) {

        return Observable.create(subscriber -> {
            try {
                connectBlocking(address, port).subscribe(subscriber::onNext, subscriber::onError);
                while (!subscriber.isUnsubscribed()) {
                    if (clientSession != null && clientSession.getSocket() != null) {
                        final DataPacket packet = clientSession.getSocket().getReceiveBuffer().poll(10, TimeUnit.MILLISECONDS);

                        if (packet != null) {
                            final Object decoded = xCoderRepository.decode(packet);
                            if (decoded != null) {
                                subscriber.onNext(new RoutedData(clientSession.getSocketID(), decoded));
                            }
                        }
                    }
                }
            }
            catch (final Exception ex) {
                subscriber.onError(ex);
            }
            subscriber.onCompleted();
            shutdown();
        });
    }

    @Override
    public void send(final Object obj, final long destId) throws IOException {
        send(obj);
    }

    public void send(final Object obj) throws BoltException {
        final Collection<DataPacket> data = xCoderRepository.encode(obj);
        for (final DataPacket dp : data) {
            try {
                send(dp);
            }
            catch (IOException ex) {
                throw new BoltException(ex);
            }
        }
//        System.out.println("DONE SENDING");
    }

    public void sendBlocking(final Object obj) throws BoltException {
        try {
            send(obj);
            flush();
        }
        catch (InterruptedException | TimeoutException | IOException ex) {
            throw new BoltException(ex);
        }
    }

    /**
     * Establishes a connection to the given server.
     * Starts the sender thread.
     *
     * @param address address of remote host.
     * @param port    port of remote host.
     * @throws UnknownHostException
     */
    private Observable<?> connectBlocking(final InetAddress address, final int port) throws InterruptedException, UnknownHostException, IOException {
        final Destination destination = new Destination(address, port);
        // Create client session
        clientSession = new ClientSession(clientEndpoint, destination);
        clientEndpoint.addSession(clientSession.getSocketID(), clientSession);

        LOG.info("The BoltClient is connecting");
        return Observable.merge(clientEndpoint.start(), clientSession.connect());
    }

    /**
     * Sends the given data asynchronously.
     *
     * @param dataPacket the data and headers to send.
     * @throws IOException
     */
    public void send(final DataPacket dataPacket) throws IOException {
        clientSession.getSocket().doWrite(dataPacket);
    }

    /**
     * flush outstanding data, with the specified maximum waiting time
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void flush() throws IOException, InterruptedException, TimeoutException {
        clientSession.getSocket().flush();
    }

    public void shutdown() {
        if (clientSession.isReady() && clientSession.isActive()) {
            Shutdown shutdown = new Shutdown();
            shutdown.setDestinationID(clientSession.getDestination().getSocketID());
            shutdown.setSession(clientSession);
            try {
                clientEndpoint.doSend(shutdown);
            }
            catch (IOException e) {
                LOG.error("ERROR: Connection could not be stopped!", e);
            }
            clientSession.getSocket().getReceiver().stop();
            clientEndpoint.stop();
        }
    }

    public BoltStatistics getStatistics() {
        return clientSession.getStatistics();
    }


}
