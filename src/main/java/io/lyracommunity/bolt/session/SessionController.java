package io.lyracommunity.bolt.session;

import io.lyracommunity.bolt.ChannelOut;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.event.ConnectionReady;
import io.lyracommunity.bolt.event.PeerDisconnected;
import io.lyracommunity.bolt.packet.BoltPacket;
import io.lyracommunity.bolt.packet.ConnectionHandshake;
import io.lyracommunity.bolt.packet.Destination;
import io.lyracommunity.bolt.packet.PacketType;
import io.lyracommunity.bolt.util.SharedCondition;
import io.lyracommunity.bolt.util.SharedCondition.PhaseStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The SessionController acts a front controller for all received packets.
 * <p>
 * The handshake negotiation process is handled here. Once a session is
 * successfully negotiated, the controller is also responsible for
 * dispatching packets to that session
 *
 * @author Cian O'Mahony
 */
public class SessionController {

    private static final Logger LOG = LoggerFactory.getLogger(SessionController.class);


    private final Config config;

    /**
     * Active sessions keyed by session ID.
     */
    private final Map<Integer, Session>     sessions               = new ConcurrentHashMap<>();
    private final Map<Destination, Session> sessionsBeingConnected = new ConcurrentHashMap<>();
    private final boolean allowAutoSessions;

    private final SharedCondition packetArrived;

    private final SharedCondition packetReady;


    public SessionController(final Config config, final boolean allowAutoSessions) {
        this.config = config;
        this.allowAutoSessions = allowAutoSessions;
        this.packetArrived = new SharedCondition(PhaseStrategy.INCREMENT);
        this.packetReady = new SharedCondition(PhaseStrategy.LATEST);
    }

    public void stop(final Subscriber<? super Object> subscriber, final String reason) {
        sessionsBeingConnected.clear();
        final Set<Integer> destIDs = sessions.keySet().stream().collect(Collectors.toSet());
        destIDs.forEach(destID -> endSession(subscriber, destID, reason));
        sessions.clear();
    }

    public boolean endSession(final Subscriber<? super Object> subscriber, final int destinationID, final String reason) {
        final Session session = sessions.remove(destinationID);
        if (session != null) session.cleanup();
        if (subscriber != null) subscriber.onNext(new PeerDisconnected(destinationID, reason));
        return (session != null);
    }

    public void processPacket(final Subscriber<? super Object> subscriber, final Destination peer,
                              final BoltPacket packet, final ChannelOut endpoint) {
        final int destID = packet.getDestinationSessionID();
        final Session session = getSession(destID);

        if (PacketType.HANDSHAKE == packet.getPacketType()) {
            if (allowAutoSessions || session != null) {
                connectionHandshake(subscriber, (ConnectionHandshake) packet, peer, endpoint);
            }
        }
        else {
            if (session != null) {
                if (PacketType.SHUTDOWN == packet.getPacketType()) {
                    endSession(subscriber, packet.getDestinationSessionID(), "Shutdown received");
                }
                // Dispatch to existing session.
                else {
                    try {
                        final boolean packetProcessed = session.received(packet);
                        if (packetProcessed) packetArrived.signal();
                    }
                    catch (Exception e) {
                        LOG.error("Unexpected error processing packet", e);
                        endSession(subscriber, packet.getDestinationSessionID(), "Unexpected error processing packet");
                    }
                }
            }
            else {
                LOG.info("Unknown session [{}] requested from [{}] - Packet Type [{}]", destID, peer, packet.getPacketType());
            }
        }
    }


    /**
     * Called whenever a connection handshake packet was received.
     *
     * @param packet   the received handshake packet.
     * @param peer     peer that sent the handshake.
     * @param endpoint the UDP endpoint.
     */
    private synchronized void connectionHandshake(final Subscriber<? super Object> subscriber,
                                                  final ConnectionHandshake packet, final Destination peer,
                                                  final ChannelOut endpoint) {
        final int sessionID = packet.getDestinationSessionID();
        Session session = getSession(sessionID);
        if (session == null) {
            session = sessionsBeingConnected.get(peer);
            // New session
            if (session == null) {
                session = new ServerSession(config, endpoint, peer);
                sessionsBeingConnected.put(peer, session);
                sessions.put(session.getSessionID(), session);
            }
            // Confirmation handshake
            else if (session.getSessionID() == sessionID) {
                sessionsBeingConnected.remove(peer);
                addSession(sessionID, session);
            }
            else if (sessionID > 0) {  // Ignore dest == 0, as it must be a duplicate client initial handshake packet.
                LOG.warn("Destination ID sent by client does not match");
                return;
            }
            final Integer peerSocketID = packet.getSessionID();
            peer.setSessionID(peerSocketID);
        }
        final boolean readyToStart = session.receiveHandshake(subscriber, packet, peer);
        if (readyToStart) {
            session.start();
            subscriber.onNext(new ConnectionReady(session));
        }
    }

    public void addSession(final Integer destinationID, final Session session) {
        LOG.info("Adding session [{}]", destinationID);
        sessions.put(destinationID, session);
    }

    public Session getSession(final Integer destinationID) {
        return sessions.get(destinationID);
    }

    public Collection<Session> getSessions() {
        return sessions.values();
    }

    public boolean awaitPacketArrival(final long timeout, final TimeUnit unit) throws InterruptedException {
        return packetArrived.awaitInterruptibly(timeout, unit);
    }

    public boolean awaitPacketReady(final long timeout, final TimeUnit unit) throws InterruptedException {
        return packetReady.awaitInterruptibly(timeout, unit);
    }

    public void signalPacketReady() {
        packetReady.signal();
    }

}
