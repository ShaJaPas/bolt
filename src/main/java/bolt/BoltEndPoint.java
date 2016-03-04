package bolt;

import bolt.event.ConnectionReadyEvent;
import bolt.packets.ConnectionHandshake;
import bolt.packets.Destination;
import bolt.packets.PacketFactory;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.net.*;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The UDPEndpoint takes care of sending and receiving UDP network packets,
 * dispatching them to the correct {@link BoltSession}
 */
public class BoltEndPoint {

    public static final int DATAGRAM_SIZE = 1400;
    private static final Logger LOG = Logger.getLogger(ClientSession.class.getName());
    final DatagramPacket dp = new DatagramPacket(new byte[DATAGRAM_SIZE], DATAGRAM_SIZE);
    private final int port;
    private final DatagramSocket dgSocket;

    /**
     * Active sessions keyed by socket ID.
     */
    private final Map<Long, BoltSession> sessions = new ConcurrentHashMap<>();

    private final Map<Destination, BoltSession> sessionsBeingConnected = new ConcurrentHashMap<>();

//    private boolean serverSocketMode = false;
//
//    private volatile boolean stopped = false;

    /**
     * create an endpoint on the given socket
     *
     * @param socket a UDP datagram socket
     */
    public BoltEndPoint(DatagramSocket socket) {
        this.dgSocket = socket;
        this.port = dgSocket.getLocalPort();
    }

    /**
     * Bind to any local port on the given host address.
     *
     * @param localAddress
     * @throws SocketException
     * @throws UnknownHostException
     */
    public BoltEndPoint(InetAddress localAddress) throws SocketException, UnknownHostException {
        this(localAddress, 0);
    }

    /**
     * Bind to the given address and port
     *
     * @param localAddress the local address to bind.
     * @param localPort    the port to bind to. If the port is zero, the system will pick an ephemeral port.
     * @throws SocketException
     * @throws UnknownHostException
     */
    public BoltEndPoint(InetAddress localAddress, int localPort) throws SocketException, UnknownHostException {
        this.dgSocket = new DatagramSocket(localPort, localAddress);

        this.port = (localPort > 0) ? localPort : dgSocket.getLocalPort();

        configureSocket();
    }

    /**
     * bind to the default network interface on the machine
     *
     * @param localPort - the port to bind to. If the port is zero, the system will pick an ephemeral port.
     * @throws SocketException
     * @throws UnknownHostException
     */
    public BoltEndPoint(int localPort) throws SocketException, UnknownHostException {
        this(null, localPort);
    }

    /**
     * bind to an ephemeral port on the default network interface on the machine
     *
     * @throws SocketException
     * @throws UnknownHostException
     */
    public BoltEndPoint() throws SocketException, UnknownHostException {
        this(null, 0);
    }

    protected void configureSocket() throws SocketException {
        // set a time out to avoid blocking in doReceive()
        dgSocket.setSoTimeout(100_000);
        // buffer size
        dgSocket.setReceiveBufferSize(128 * 1024);
        dgSocket.setReuseAddress(false);
    }

    /**
     * Start the endpoint.
     */
    public Observable<?> start() {
        return Observable.<Object>create(this::doReceive).subscribeOn(Schedulers.io());
    }

    public void stop() {
//        stopped = true;
        dgSocket.close();
    }

    /**
     * @return the port which this client is bound to
     */
    public int getLocalPort() {
        return this.dgSocket.getLocalPort();
    }

    /**
     * @return Gets the local address to which the socket is bound
     */
    public InetAddress getLocalAddress() {
        return this.dgSocket.getLocalAddress();
    }

    DatagramSocket getSocket() {
        return dgSocket;
    }

    public void addSession(Long destinationID, BoltSession session) {
        LOG.info("Storing session <" + destinationID + ">");
        sessions.put(destinationID, session);
    }

    public BoltSession getSession(Long destinationID) {
        return sessions.get(destinationID);
    }

    public Collection<BoltSession> getSessions() {
        return sessions.values();
    }

    /**
     * Single receive, run in the receiverThread, see {@link #start()}.
     * <ul>
     * <li>Receives UDP packets from the network.
     * <li>Converts them to Bolt packets.
     * <li>dispatches the Bolt packets according to their destination ID.
     * </ul>
     *
     * @throws IOException
     */
    protected void doReceive(final Subscriber<? super Object> subscriber) {
        LOG.info("BoltEndpoint started.");
        while (!subscriber.isUnsubscribed()) {
            try {
                // Will block until a packet is received or timeout has expired.
                dgSocket.receive(dp);

                final Destination peer = new Destination(dp.getAddress(), dp.getPort());
                final int l = dp.getLength();
                final BoltPacket packet = PacketFactory.createPacket(dp.getData(), l);

                long dest = packet.getDestinationID();
                final BoltSession session = sessions.get(dest);

                if (packet.isConnectionHandshake()) {
                    final BoltSession result = connectionHandshake(subscriber, (ConnectionHandshake) packet, peer, session);
                    if (result.isReady()) subscriber.onNext(new ConnectionReadyEvent(result));
                }
                else if (session != null) {
                    // Dispatch to existing session.
                    session.received(packet, peer);
                }
                else {
                    LOG.warning("Unknown session <" + dest + "> requested from <" + peer + "> packet type " + packet.getClass().getName());
                }
            }
            catch (SocketException | SocketTimeoutException ex) {
                // For timeout, can safely ignore... will retry until the endpoint is stopped.
                LOG.log(Level.INFO, "SocketException: " + ex.getMessage());
            }
            catch (Exception ex) {
                LOG.log(Level.WARNING, "Got: " + ex.getMessage(), ex);
            }
        }
        stop();
    }

    /**
     * Called when a "connection handshake" packet was received and no
     * matching session yet exists.
     *
     * @param packet
     * @param peer
     * @throws IOException
     * @throws InterruptedException
     */
    protected synchronized BoltSession connectionHandshake(final Subscriber<? super Object> subscriber,
                                                           final ConnectionHandshake packet, final Destination peer,
                                                           final BoltSession existingSession) throws IOException, InterruptedException {
        BoltSession session = existingSession;
        if (session == null) {
            final Destination p = new Destination(peer.getAddress(), peer.getPort());
            session = sessionsBeingConnected.get(peer);
            final long destID = packet.getDestinationID();
            // New session
            if (session == null) {
                session = new ServerSession(peer, this);
                sessionsBeingConnected.put(p, session);
                sessions.put(session.getSocketID(), session);
            }
            // Confirmation handshake
            else if (session.getSocketID() == destID) {
                sessionsBeingConnected.remove(p);
                addSession(destID, session);
            }
            else {
                throw new IOException("dest ID sent by client does not match");
            }
            Integer peerSocketID = packet.getSocketID();
            peer.setSocketID(peerSocketID);
        }
        session.receiveHandshake(subscriber, packet, peer);
        return session;
    }

    protected void doSend(BoltPacket packet) throws IOException {
        byte[] data = packet.getEncoded();
        DatagramPacket dgp = packet.getSession().getDatagram();
        dgp.setData(data);
        dgSocket.send(dgp);
    }

    public String toString() {
        return "UDPEndpoint port=" + port;
    }

    public void sendRaw(DatagramPacket p) throws IOException {
        dgSocket.send(p);
    }

}
