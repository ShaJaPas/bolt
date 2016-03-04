package bolt;

import bolt.packets.DataPacket;
import bolt.packets.Destination;
import bolt.packets.Shutdown;
import bolt.statistic.BoltStatistics;
import bolt.xcoder.MessageAssembleBuffer;
import bolt.xcoder.XCoderRepository;
import rx.Observable;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BoltClient implements Client {


    private static final Logger LOGGER = Logger.getLogger(BoltClient.class.getName());

    private final BoltEndPoint          clientEndpoint;
    private final XCoderRepository      xCoderRepository;
    private       ClientSession         clientSession;


    public BoltClient(final InetAddress address, final int localPort) throws SocketException, UnknownHostException {
        this(new BoltEndPoint(address, localPort));
    }

    public BoltClient(final InetAddress address) throws SocketException, UnknownHostException {
        this(new BoltEndPoint(address));
    }

    public BoltClient(final BoltEndPoint endpoint) throws SocketException, UnknownHostException {
        this.clientEndpoint = endpoint;
        this.xCoderRepository = XCoderRepository.create(new MessageAssembleBuffer());
        LOGGER.info("Created client endpoint on port " + clientEndpoint.getLocalPort());
    }


    @Override
    public Observable<?> connect(final InetAddress address, final int port) {

        return Observable.create(subscriber -> {
            try {
                connectBlocking(address, port).subscribe(subscriber);
                while (!subscriber.isUnsubscribed()) {
                    final DataPacket packet = clientSession.getSocket().getReceiveBuffer().poll(10, TimeUnit.MILLISECONDS);

                    if (packet != null) {
                        final Object decoded = xCoderRepository.decode(packet);
                        if (decoded != null) {
                            subscriber.onNext(decoded);
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
        final Collection<DataPacket> data = xCoderRepository.encode(obj);
        for (final DataPacket dp : data) {
            send(dp);
        }
    }

    /**
     * Establishes a connection to the given server.
     * Starts the sender thread.
     *
     * @param address
     * @param port
     * @throws UnknownHostException
     */
    private Observable<?> connectBlocking(final InetAddress address, final int port) throws InterruptedException, UnknownHostException, IOException {
        final Destination destination = new Destination(address, port);
        //create client session...
        clientSession = new ClientSession(clientEndpoint, destination);
        clientEndpoint.addSession(clientSession.getSocketID(), clientSession);
        final Observable<?> endpointEvents = clientEndpoint.start();
        clientSession.connect();
        //wait for handshake
        while (!clientSession.isReady()) { //TODO #connect already blocks waiting for ready, why twice?
            Thread.sleep(50);
        }
        LOGGER.info("The BoltClient is connected");
        return endpointEvents;
    }

    /**
     * Sends the given data asynchronously.
     *
     * @param dataPacket the data and headers to send.
     * @throws IOException
     */
    private void send(final DataPacket dataPacket) throws IOException {
        clientSession.getSocket().doWrite(dataPacket);
    }

    public void send(byte[] data) throws IOException {
        clientSession.getSocket().doWrite(data);
    }

    /**
     * sends the given data and waits for acknowledgement
     *
     * @param data the data to send
     * @throws IOException
     * @throws InterruptedException if interrupted while waiting for ack
     */
    public void sendBlocking(byte[] data) throws IOException, InterruptedException {
        clientSession.getSocket().doWriteBlocking(data);
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
                LOGGER.log(Level.SEVERE, "ERROR: Connection could not be stopped!", e);
            }
            clientSession.getSocket().getReceiver().stop();
            clientEndpoint.stop();
        }
    }

    public BoltStatistics getStatistics() {
        return clientSession.getStatistics();
    }


}
