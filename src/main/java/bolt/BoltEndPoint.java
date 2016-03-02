package bolt;

import bolt.packets.ConnectionHandshake;
import bolt.packets.Destination;
import bolt.packets.PacketFactory;
import bolt.util.BoltThreadFactory;

import java.io.IOException;
import java.net.*;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
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

    /**
     * If the endpoint is configured for a server socket, this queue is used to hand-off
     * new BoltSessions to the application.
     */
    private final SynchronousQueue<BoltSession> sessionHandOff = new SynchronousQueue<>();

    private boolean serverSocketMode = false;

    private volatile boolean stopped = false;

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
     * start the endpoint. If the serverSocketModeEnabled flag is true,
     * a new connection can be handed off to an application. The application needs to
     * call #accept() to get the socket
     *
     * @param serverSocketModeEnabled
     */
    public void start(boolean serverSocketModeEnabled) {
        serverSocketMode = serverSocketModeEnabled;
        // Start receive thread
        final Thread t = BoltThreadFactory.get().newThread(this::doReceive, "UDPEndpoint", true);
        t.start();
        LOG.info("BoltEndpoint started.");
    }

    public void start() {
        start(false);
    }

    public void stop() {
        stopped = true;
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
     * Wait the given time for a new connection.
     *
     * @param timeout the time to wait
     * @param unit    the {@link TimeUnit}
     * @return a new {@link BoltSession}
     * @throws InterruptedException
     */
    protected BoltSession accept(long timeout, TimeUnit unit) throws InterruptedException {
        return sessionHandOff.poll(timeout, unit);
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
    protected void doReceive() {
        while (!stopped) {
            try {
                //will block until a packet is received or timeout has expired
                dgSocket.receive(dp);

                final Destination peer = new Destination(dp.getAddress(), dp.getPort());
                final int l = dp.getLength();
                final BoltPacket packet = PacketFactory.createPacket(dp.getData(), l);

                long dest = packet.getDestinationID();
                final BoltSession session = sessions.get(dest);
                if (session != null) {
                    // dispatch to existing session
                    session.received(packet, peer);
                }
                else if (packet.isConnectionHandshake()) {
                    connectionHandshake((ConnectionHandshake) packet, peer);
                }
                else {
                    LOG.warning("Unknown session <" + dest + "> requested from <" + peer + "> packet type " + packet.getClass().getName());
                }
            }
            catch (SocketException ex) {
                LOG.log(Level.INFO, "SocketException: " + ex.getMessage());
            }
            catch (SocketTimeoutException ste) {
                //can safely ignore... we will retry until the endpoint is stopped
            }
            catch (Exception ex) {
                LOG.log(Level.WARNING, "Got: " + ex.getMessage(), ex);
            }
        }
    }

    /**
     * called when a "connection handshake" packet was received and no
     * matching session yet exists
     *
     * @param packet
     * @param peer
     * @throws IOException
     * @throws InterruptedException
     */
    protected synchronized void connectionHandshake(ConnectionHandshake packet, Destination peer) throws IOException, InterruptedException {
        final Destination p = new Destination(peer.getAddress(), peer.getPort());
        BoltSession session = sessionsBeingConnected.get(peer);
        long destID = packet.getDestinationID();
        if (session != null && session.getSocketID() == destID) {
            // Confirmation handshake
            sessionsBeingConnected.remove(p);
            addSession(destID, session);
        }
        else if (session == null) {
            session = new ServerSession(peer, this);
            sessionsBeingConnected.put(p, session);
            sessions.put(session.getSocketID(), session);
            if (serverSocketMode) {
                LOG.fine("Pooling new request.");
                sessionHandOff.put(session);
                LOG.fine("Request taken for processing.");
            }
        }
        else {
            throw new IOException("dest ID sent by client does not match");
        }
        Integer peerSocketID = packet.getSocketID();
        peer.setSocketID(peerSocketID);
        session.received(packet, peer);
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
