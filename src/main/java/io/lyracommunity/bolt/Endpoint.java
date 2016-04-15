package io.lyracommunity.bolt;

import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.event.ConnectionReady;
import io.lyracommunity.bolt.event.PeerDisconnected;
import io.lyracommunity.bolt.packet.*;
import io.lyracommunity.bolt.session.ServerSession;
import io.lyracommunity.bolt.session.Session;
import io.lyracommunity.bolt.session.SessionState;
import io.lyracommunity.bolt.util.NetworkQoSSimulationPipeline;
import io.lyracommunity.bolt.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.net.*;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The UDPEndpoint takes care of sending and receiving UDP network packets,
 * dispatching them to the correct {@link Session}
 */
public class Endpoint implements ChannelOut {

    private static final Logger LOG = LoggerFactory.getLogger(Endpoint.class);

    private final DatagramPacket dp = new DatagramPacket(new byte[Config.DEFAULT_DATAGRAM_SIZE], Config.DEFAULT_DATAGRAM_SIZE);
    private final int            port;
    private final DatagramSocket dgSocket;
    private final Config         config;
    private final String         name;

    /**
     * Active sessions keyed by socket ID.
     */
    private final Map<Integer, Session> sessions = new ConcurrentHashMap<>();

    private final Map<Destination, Session> sessionsBeingConnected = new ConcurrentHashMap<>();

    private final Map<Integer, Subscription> sessionSubscriptions = new ConcurrentHashMap<>();

    /**
     * Bind to the given address and port.
     *
     * @param name   name of the endpoint, eg "ClientEndpoint" or "ServerEndpoint".
     * @param config config containing the address information to bind.
     * @throws SocketException if for example if the port is already bound to.
     */
    public Endpoint(final String name, final Config config) throws SocketException {
        this.config = config;
        this.name = name;
        this.dgSocket = new DatagramSocket(config.getLocalPort(), config.getLocalAddress());
        // If the port is zero, the system will pick an ephemeral port.
        this.port = (config.getLocalPort() > 0) ? config.getLocalPort() : dgSocket.getLocalPort();
        configureSocket();
    }

    private void configureSocket() throws SocketException {
        // set a time out to avoid blocking in doReceive()
        dgSocket.setSoTimeout(50_000);
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

    public void stop(final Subscriber<? super Object> subscriber) {
        LOG.info("Stopping {}", name);
        sessionsBeingConnected.clear();
        final Set<Integer> destIDs = Stream.concat(sessions.keySet().stream(), sessionSubscriptions.keySet().stream())
                .collect(Collectors.toSet());
        destIDs.forEach(destID -> endSession(subscriber, destID, name + " is closing."));
        sessions.clear();
        sessionSubscriptions.clear();
        dgSocket.close();
    }

    private void endSession(final Subscriber<? super Object> subscriber, final int destinationID, final String reason) {
        final Session session = sessions.remove(destinationID);
        final Subscription sessionSub = sessionSubscriptions.remove(destinationID);
        if (session != null) session.cleanup();
        if (sessionSub != null) sessionSub.unsubscribe();
        if (subscriber != null) subscriber.onNext(new PeerDisconnected(destinationID, reason));
    }

    @Override
    public int getLocalPort() {
        return this.dgSocket.getLocalPort();
    }

    DatagramSocket getSocket() {
        return dgSocket;
    }

    void addSession(final Integer destinationID, final Session session) {
        LOG.info("{} is adding session [{}]", name, destinationID);
        sessions.put(destinationID, session);
    }

    Session getSession(final Integer destinationID) {
        return sessions.get(destinationID);
    }

    Collection<Session> getSessions() {
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
    private void doReceive(final Subscriber<? super Object> subscriber) {
        Thread.currentThread().setName("Bolt-" + name + "-" + Util.THREAD_INDEX.incrementAndGet());
        LOG.info("{} started.", name);

        final NetworkQoSSimulationPipeline qosSimulationPipeline = new NetworkQoSSimulationPipeline(config,
                (peer, pkt) -> processPacket(subscriber, peer, pkt), (peer, pkt) -> markPacketAsDropped(pkt));
        while (!subscriber.isUnsubscribed()) {
            try {
                // Will block until a packet is received or timeout has expired.
                dgSocket.receive(dp);

                final Destination peer = new Destination(dp.getAddress(), dp.getPort());
                final int l = dp.getLength();
                final BoltPacket packet = PacketFactory.createPacket(dp.getData(), l);

                if (LOG.isDebugEnabled()) LOG.debug("{} received packet {}", name, packet.getPacketSeqNumber());

                qosSimulationPipeline.offer(peer, packet);

            }
            catch (AsynchronousCloseException ex) {
                LOG.info("{} interrupted.", name);
            }
            catch (SocketTimeoutException ex) {
                LOG.debug("{} socket timeout", name);
            }
            catch (SocketException ex) {
                LOG.warn("{} SocketException: {}", name, ex.getMessage());
                if (dgSocket.isClosed()) subscriber.onError(ex);
            }
            catch (ClosedChannelException ex) {
                LOG.warn("{} Channel was closed but receive was attempted", name);
            }
            catch (Exception ex) {
                LOG.error("{} Unexpected endpoint error", name, ex);
            }
        }
        LOG.info("{} completed naturally.", name);
        qosSimulationPipeline.close();
        stop(subscriber);
    }

    private void markPacketAsDropped(final BoltPacket packet) {
        final Session session = sessions.get(packet.getDestinationID());
        if (session != null) session.getStatistics().incNumberOfArtificialDrops();
    }

    private void processPacket(final Subscriber<? super Object> subscriber, final Destination peer, final BoltPacket packet) {
        final int destID = packet.getDestinationID();
        final Session session = getSession(destID);

        if (PacketType.HANDSHAKE == packet.getPacketType()) {
            connectionHandshake(subscriber, (ConnectionHandshake) packet, peer, session);
        }
        else if (session != null) {
            if (PacketType.SHUTDOWN == packet.getPacketType()) {
                endSession(subscriber, packet.getDestinationID(), "Shutdown received");
            }
            // Dispatch to existing session.
            else {
                session.received(packet, subscriber);
            }
        }
        else {
            LOG.info("Unknown session [{}] requested from [{}] - Packet Type [{}]", destID, peer, packet.getPacketType());
        }
    }

    /**
     * Called when a "connection handshake" packet was received and no
     * matching session yet exists.
     *
     * @param packet the received handshake packet.
     * @param peer   peer that sent the handshake.
     * @return true if connection is ready to start, otherwise false.
     * @throws IOException
     * @throws InterruptedException
     */
    private synchronized Session connectionHandshake(final Subscriber<? super Object> subscriber,
                                                     final ConnectionHandshake packet, final Destination peer,
                                                     final Session existingSession) {
        final int destID = packet.getDestinationID();
        Session session = existingSession;
        if (session == null) {
            session = sessionsBeingConnected.get(peer);
            // New session
            if (session == null) {
                session = new ServerSession(config, this, peer);
                sessionsBeingConnected.put(peer, session);
                sessions.put(session.getSocketID(), session);
            }
            // Confirmation handshake
            else if (session.getSocketID() == destID) {
                sessionsBeingConnected.remove(peer);
                addSession(destID, session);
            }
            else if (destID > 0) {  // Ignore dest == 0, as it must be a duplicate client initial handshake packet.
                subscriber.onError(new IOException("Destination ID sent by client does not match"));
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
            subscriber.onNext(new ConnectionReady(session));
        }
        return session;
    }

    @Override
    public InetAddress getLocalAddress() {
        return this.dgSocket.getLocalAddress();
    }

    @Override
    public boolean isOpen() {
        return !dgSocket.isClosed();
    }

    @Override
    public void doSend(final BoltPacket packet, final SessionState sessionState) throws IOException {
        final byte[] data = packet.getEncoded();
        final DatagramPacket dgp = sessionState.getDatagram();
        dgp.setData(data);
        dgSocket.send(dgp);
    }

    public String toString() {
        return name + " port=" + port;
    }

    void sendRaw(DatagramPacket p) throws IOException {
        dgSocket.send(p);
    }

    public Config getConfig() {
        return config;
    }

}
