package bolt;

import bolt.packets.Destination;
import bolt.packets.Shutdown;
import bolt.statistic.BoltStatistics;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BoltClient {

    private static final Logger logger = Logger.getLogger(BoltClient.class.getName());
    private final BoltEndPoint clientEndpoint;
    private ClientSession clientSession;


    public BoltClient(InetAddress address, int localport) throws SocketException, UnknownHostException {
        //create endpoint
        clientEndpoint = new BoltEndPoint(address, localport);
        logger.info("Created client endpoint on port " + localport);
    }

    public BoltClient(InetAddress address) throws SocketException, UnknownHostException {
        //create endpoint
        clientEndpoint = new BoltEndPoint(address);
        logger.info("Created client endpoint on port " + clientEndpoint.getLocalPort());
    }

    public BoltClient(BoltEndPoint endpoint) throws SocketException, UnknownHostException {
        clientEndpoint = endpoint;
    }

    /**
     * establishes a connection to the given server.
     * Starts the sender thread.
     *
     * @param host
     * @param port
     * @throws UnknownHostException
     */
    public void connect(String host, int port) throws InterruptedException, UnknownHostException, IOException {
        InetAddress address = InetAddress.getByName(host);
        Destination destination = new Destination(address, port);
        //create client session...
        clientSession = new ClientSession(clientEndpoint, destination);
        clientEndpoint.addSession(clientSession.getSocketID(), clientSession);
        clientEndpoint.start();
        clientSession.connect();
        //wait for handshake
        while (!clientSession.isReady()) {
            Thread.sleep(50);
        }
        logger.info("The BoltClient is connected");
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
     * @param data - the data to send
     * @throws IOException
     * @throws InterruptedException if interrupted while waiting for ack
     */
    public void sendBlocking(byte[] data) throws IOException, InterruptedException {
        clientSession.getSocket().doWriteBlocking(data);
    }

    public int read(byte[] data) throws IOException, InterruptedException {
        return clientSession.getSocket().getInputStream().read(data);
    }

    /**
     * flush outstanding data, with the specified maximum waiting time
     *
     * @param timeOut - timeout in millis (if smaller than 0, no timeout is used)
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
            } catch (IOException e) {
                logger.log(Level.SEVERE, "ERROR: Connection could not be stopped!", e);
            }
            clientSession.getSocket().getReceiver().stop();
            clientEndpoint.stop();
        }
    }

    public BoltInputStream getInputStream() throws IOException {
        return clientSession.getSocket().getInputStream();
    }

    public BoltOutputStream getOutputStream() throws IOException {
        return clientSession.getSocket().getOutputStream();
    }

    public BoltEndPoint getEndpoint() throws IOException {
        return clientEndpoint;
    }

    public BoltStatistics getStatistics() {
        return clientSession.getStatistics();
    }

}
