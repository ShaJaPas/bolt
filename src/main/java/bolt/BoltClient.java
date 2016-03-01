package bolt;

import bolt.packets.DataPacket;
import bolt.packets.Destination;
import bolt.packets.Shutdown;
import bolt.statistic.BoltStatistics;
import bolt.util.MessageAssembler;
import bolt.xcoder.Client;
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

    private final BoltEndPoint clientEndpoint;
    private final XCoderRepository xCoderRepository;
    private final MessageAssembler messageAssembler;
    private ClientSession clientSession;


    public BoltClient(InetAddress address, int localPort) throws SocketException, UnknownHostException {
        this.clientEndpoint = new BoltEndPoint(address, localPort);
        this.xCoderRepository = new XCoderRepository();
        LOGGER.info("Created client endpoint on port " + localPort);
    }

    public BoltClient(InetAddress address) throws SocketException, UnknownHostException {
        this.clientEndpoint = new BoltEndPoint(address);
        this.xCoderRepository = new XCoderRepository();
        LOGGER.info("Created client endpoint on port " + clientEndpoint.getLocalPort());
    }

    public BoltClient(BoltEndPoint endpoint) throws SocketException, UnknownHostException {
        this.clientEndpoint = endpoint;
        this.xCoderRepository = new XCoderRepository();
    }


    @Override
    public Observable<?> connect(final InetAddress address, final int port) {

        return Observable.create(subscriber -> {
            try {
                connectBlocking(address, port);
                while (!subscriber.isUnsubscribed()) {
                    final DataPacket packet = clientSession.getSocket().getReceiveBuffer().poll(10, TimeUnit.MILLISECONDS);

                    if (packet != null) {
                        Object decoded = null;
                        // If message, add part to assembly.
                        if(packet.isMessage()) {
                            decoded = messageAssembler.addChunk(packet.getData(), packet.getMessageChunkNumber(),
                                    packet.isFinalMessageChunk());
                        }
                        // If not, decode and send.
                        else {
                            decoded = xCoderRepository.decode(packet.getData());
                        }

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
        });
    }

    @Override
    public void send(final Object obj, final long destId, final boolean reliable) {
        final Collection<byte[]> data = xCoderRepository.encode(obj);
        send(data, reliable);
    }

    /**
     * Establishes a connection to the given server.
     * Starts the sender thread.
     *
     * @param address
     * @param port
     * @throws UnknownHostException
     */
    private void connectBlocking(InetAddress address, int port) throws InterruptedException, UnknownHostException, IOException {
        Destination destination = new Destination(address, port);
        //create client session...
        clientSession = new ClientSession(clientEndpoint, destination);
        clientEndpoint.addSession(clientSession.getSocketID(), clientSession);
        clientEndpoint.start();
        clientSession.connect();
        //wait for handshake
        while (!clientSession.isReady()) { //TODO #connect already blocks waiting for ready, why twice?
            Thread.sleep(50);
        }
        LOGGER.info("The BoltClient is connected");
    }

    public void connect(final String host, final int port) throws InterruptedException, UnknownHostException, IOException {
        connectBlocking(InetAddress.getByName(host), port);
    }

    /**
     * sends the given data asynchronously
     *
     * @param data - the data to send
     * @throws IOException
     */
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

    public void shutdown() throws IOException {

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
