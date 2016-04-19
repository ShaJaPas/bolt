package io.lyracommunity.bolt.session;

import io.lyracommunity.bolt.ChannelOut;
import io.lyracommunity.bolt.SessionWorkMediator;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.event.ConnectionReady;
import io.lyracommunity.bolt.event.PeerDisconnected;
import io.lyracommunity.bolt.packet.BoltPacket;
import io.lyracommunity.bolt.packet.ConnectionHandshake;
import io.lyracommunity.bolt.packet.Destination;
import io.lyracommunity.bolt.packet.PacketType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;
import rx.Subscription;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by keen on 19/04/16.
 */
public class SessionController {

    private static final Logger LOG = LoggerFactory.getLogger(SessionController.class);


    private final Config config;

    private final SessionWorkMediator mediator;
    /**
     * Active sessions keyed by socket ID.
     */
    private final Map<Integer, Session>      sessions               = new ConcurrentHashMap<>();
    private final Map<Destination, Session>  sessionsBeingConnected = new ConcurrentHashMap<>();
    private final Map<Integer, Subscription> sessionSubscriptions   = new ConcurrentHashMap<>();
    private final boolean allowAutoSessions;


    public SessionController(final Config config, final boolean allowAutoSessions) {
        this.config = config;
        this.allowAutoSessions = allowAutoSessions;
        this.mediator = new SessionWorkMediator();
    }

    public void stop(final Subscriber<? super Object> subscriber, final String reason) {
        sessionsBeingConnected.clear();
        final Set<Integer> destIDs = Stream.concat(sessions.keySet().stream(), sessionSubscriptions.keySet().stream())
                .collect(Collectors.toSet());
        destIDs.forEach(destID -> endSession(subscriber, destID, reason));
        sessions.clear();
        sessionSubscriptions.clear();
    }

    private void endSession(final Subscriber<? super Object> subscriber, final int destinationID, final String reason) {
        final Session session = sessions.remove(destinationID);
        final Subscription sessionSub = sessionSubscriptions.remove(destinationID);
        if (session != null) session.cleanup();
        if (sessionSub != null) sessionSub.unsubscribe();
        if (subscriber != null) subscriber.onNext(new PeerDisconnected(destinationID, reason));
    }

    public void processPacket(final Subscriber<? super Object> subscriber, final Destination peer,
                              final BoltPacket packet, final ChannelOut endpoint) {
        final int destID = packet.getDestinationID();
        final Session session = getSession(destID);

        if (PacketType.HANDSHAKE == packet.getPacketType()) {
            if (allowAutoSessions || session != null) {
                connectionHandshake(subscriber, (ConnectionHandshake) packet, peer, endpoint);
            }
        }
        else {
            if (session != null) {
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
    }


    /**
     * Called when a "connection handshake" packet was received and no
     * matching session yet exists.
     *
     * @param packet   the received handshake packet.
     * @param peer     peer that sent the handshake.
     * @param endpoint the UDP endpoint.
     */
    private synchronized void connectionHandshake(final Subscriber<? super Object> subscriber,
                                                  final ConnectionHandshake packet, final Destination peer,
                                                  final ChannelOut endpoint) {
        final int destID = packet.getDestinationID();
        Session session = getSession(destID);
        if (session == null) {
            session = sessionsBeingConnected.get(peer);
            // New session
            if (session == null) {
                session = new ServerSession(config, endpoint, peer);
                sessionsBeingConnected.put(peer, session);
                sessions.put(session.getSocketID(), session);
            }
            // Confirmation handshake
            else if (session.getSocketID() == destID) {
                sessionsBeingConnected.remove(peer);
                addSession(destID, session);
            }
            else if (destID > 0) {  // Ignore dest == 0, as it must be a duplicate client initial handshake packet.
                LOG.warn("Destination ID sent by client does not match");
                return;
            }
            final Integer peerSocketID = packet.getSocketID();
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

    public boolean awaitMoreWork(int timeout, TimeUnit unit) throws InterruptedException {
        return mediator.awaitMoreWork(timeout, unit);
    }

}
