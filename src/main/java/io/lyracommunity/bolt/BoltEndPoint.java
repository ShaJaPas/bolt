package io.lyracommunity.bolt;

import io.lyracommunity.bolt.event.ConnectionReady;
import io.lyracommunity.bolt.event.PeerDisconnected;
import io.lyracommunity.bolt.packet.BoltPacket;
import io.lyracommunity.bolt.packet.ConnectionHandshake;
import io.lyracommunity.bolt.packet.ControlPacketType;
import io.lyracommunity.bolt.packet.Destination;
import io.lyracommunity.bolt.packet.PacketFactory;
import io.lyracommunity.bolt.session.BoltSession;
import io.lyracommunity.bolt.session.ServerSession;
import io.lyracommunity.bolt.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketOptions;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The UDPEndpoint takes care of sending and receiving UDP network packets,
 * dispatching them to the correct {@link BoltSession}
 */
public class BoltEndPoint {

    public static final int DATAGRAM_SIZE = 1400;
    private static final Logger LOG = LoggerFactory.getLogger(BoltEndPoint.class);
    private final DatagramPacket dp = new DatagramPacket(new byte[DATAGRAM_SIZE], DATAGRAM_SIZE);
    private final int port;
    private final DatagramChannel dgChannel;
    private final DatagramSocket dgSocket;
    private final ByteBuffer receiveBuffer = ByteBuffer.allocate(128 * 1024);
    private final Config config;

    /**
     * Active sessions keyed by socket ID.
     */
    private final Map<Long, BoltSession> sessions = new ConcurrentHashMap<>();

    private final Map<Destination, BoltSession> sessionsBeingConnected = new ConcurrentHashMap<>();

    private final Map<Long, Subscription> sessionSubscriptions = new ConcurrentHashMap<>();

    /**
     * Bind to the given address and port
     *
     * @param localAddress the local address to bind.
     * @param localPort    the port to bind to. If the port is zero, the system will pick an ephemeral port.
     * @throws SocketException      if for example if the port is already bound to.
     * @throws UnknownHostException
     */
    public BoltEndPoint(final InetAddress localAddress, final int localPort) throws SocketException, UnknownHostException, IOException {
        this(new Config(localAddress, localPort));
    }

    /**
     * Bind to the given address and port.
     *
     * @param config config containing the address information to bind.
     * @throws SocketException
     * @throws UnknownHostException
     */
    public BoltEndPoint(final Config config) throws UnknownHostException, SocketException, IOException {
        this.config = config;
        this.dgChannel = DatagramChannel.open();
        this.dgSocket = dgChannel.socket();
//        this.dgSocket = new DatagramSocket(config.getLocalPort(), config.getLocalAddress());
        // If the port is zero, the system will pick an ephemeral port.
        this.port = (config.getLocalPort() > 0) ? config.getLocalPort() : dgSocket.getLocalPort();
        configureSocket();
        this.dgSocket.bind(new InetSocketAddress(config.getLocalAddress(), config.getLocalPort()));
    }

    protected void configureSocket() throws SocketException, IOException {
        dgChannel.setOption(StandardSocketOptions.SO_REUSEADDR, false);
        dgChannel.setOption(StandardSocketOptions.SO_RCVBUF, 128 * 1024);
        // set a time out to avoid blocking in doReceive()
//        dgSocket.setSoTimeout(100_000);
        // buffer size
//        dgSocket.setReceiveBufferSize(128 * 1024);
//        dgSocket.setReuseAddress(false);
    }

    /**
     * Start the endpoint.
     */
    public Observable<?> start() {
        return Observable.<Object>create(this::doReceive).subscribeOn(Schedulers.io());
    }

    private void stop(final Subscriber<? super Object> subscriber) {
        sessionsBeingConnected.clear();
        final Set<Long> destIDs = Stream.concat(sessions.keySet().stream(), sessionSubscriptions.keySet().stream())
                .collect(Collectors.toSet());
        destIDs.forEach(destID -> endSession(subscriber, destID, "Endpoint is closing."));
        sessions.clear();
        sessionSubscriptions.clear();
        dgSocket.close();
    }

    private void endSession(final Subscriber<? super Object> subscriber, final long destinationID, final String reason) {
        final BoltSession session = sessions.remove(destinationID);
        final Subscription sessionSub = sessionSubscriptions.remove(destinationID);
        System.out.println("REMOVED " + destinationID + " " + reason + " " + session);
        if (session != null) session.close();
        if (sessionSub != null) sessionSub.unsubscribe();
        if (subscriber != null) subscriber.onNext(new PeerDisconnected(destinationID, reason));
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

    public void addSession(final Long destinationID, final BoltSession session) {
        LOG.info("Storing session [{}]", destinationID);
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
     */
    protected void doReceive(final Subscriber<? super Object> subscriber) {
        Thread.currentThread().setName("Bolt-Endpoint" + Util.THREAD_INDEX.incrementAndGet());
        LOG.info("BoltEndpoint started.");
        System.out.println("ENDPOINT START " + this);
        while (!subscriber.isUnsubscribed()) {
            try {
                // Will block until a packet is received or timeout has expired.
//                receiveBuffer.clear();
//                final InetSocketAddress fromAddress = (InetSocketAddress) dgChannel.receive(receiveBuffer);
                dgSocket.receive(dp);

                final Destination peer = new Destination(dp.getAddress(), dp.getPort());
                final int l = dp.getLength();
                final BoltPacket packet = PacketFactory.createPacket(dp.getData(), l);
//                final Destination peer = new Destination(fromAddress.getAddress(), fromAddress.getPort());
//                final int l = receiveBuffer.

                if (LOG.isDebugEnabled()) LOG.debug("Received packet {}", packet.getPacketSeqNumber());

                final long destID = packet.getDestinationID();
                final BoltSession session = sessions.get(destID);

                final boolean isConnectionHandshake = ControlPacketType.CONNECTION_HANDSHAKE.getTypeId() == packet.getControlPacketType();
                if (isConnectionHandshake) {
                    final BoltSession result = connectionHandshake(subscriber, (ConnectionHandshake) packet, peer, session);
                    if (result.isReady()) subscriber.onNext(new ConnectionReady(result));
                }
                else if (session != null) {
                    if (ControlPacketType.SHUTDOWN.getTypeId() == packet.getControlPacketType()) {
                        endSession(subscriber, packet.getDestinationID(), "Shutdown received");
                    }
                    // Dispatch to existing session.
                    else {
                        session.received(packet, peer, subscriber);
                    }
                }
                else {
                    LOG.warn("Unknown session [{}] requested from [{}] - Packet Type [{}]", destID, peer, packet.getClass().getName());
                }
            }
            catch (InterruptedException ex) {
                System.out.println("\tENDPOINT INTERRUPT");
                LOG.info("Endpoint interrupted {}", ex);
            }
            catch (SocketTimeoutException ex) {
                LOG.debug("Endpoint socket timeout");
            }
            catch (SocketException ex) {
                // For timeout, can safely ignore... will retry until the endpoint is stopped.
                LOG.warn("SocketException: {}", ex);
//                if (dgSocket.isClosed()) subscriber.unsubscribe();
            }
            catch (Exception ex) {
                LOG.error("Unexpected endpoint error", ex);
            }
            System.out.println("ENDPOINT LOOP");
        }
        System.out.println("STOP ENDPOINT " + this);
        stop(subscriber);
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
        final long destID = packet.getDestinationID();
        BoltSession session = existingSession;
        if (session == null) {
            final Destination p = new Destination(peer.getAddress(), peer.getPort());
            session = sessionsBeingConnected.get(peer);
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
                throw new IOException("Destination ID sent by client does not match");
            }
            Integer peerSocketID = packet.getSocketID();
            peer.setSocketID(peerSocketID);
        }
        final boolean readyToStart = session.receiveHandshake(subscriber, packet, peer);
        if (readyToStart) {
            final Subscription sessionSubscription = session.start().subscribe(subscriber::onNext,
                    ex -> endSession(subscriber, destID, ex.getMessage()),
                    () -> endSession(subscriber, destID, "Session ended successfully"));
            sessionSubscriptions.put(destID, sessionSubscription);
            System.out.println("CREATED SESSION " + session);
        }
        return session;
    }

    public void doSend(final BoltPacket packet, final BoltSession session) throws IOException {
        final byte[] data = packet.getEncoded();
        DatagramPacket dgp = session.getDatagram();
        dgp.setData(data);
        dgSocket.send(dgp);
    }

    public String toString() {
        return "BoltEndpoint port=" + port;
    }

    public void sendRaw(DatagramPacket p) throws IOException {
        dgSocket.send(p);
    }

    public Config getConfig() {
        return config;
    }

}
