package io.lyracommunity.bolt.session;

import io.lyracommunity.bolt.ChannelOut;
import io.lyracommunity.bolt.api.BoltEvent;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.packet.ConnectionHandshake;
import io.lyracommunity.bolt.packet.Destination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.lyracommunity.bolt.session.SessionStatus.*;

/**
 * Client side of a client-server Bolt connection.
 */
public class ClientSession extends Session {

    private static final Logger LOG = LoggerFactory.getLogger(ClientSession.class);

    private final Phaser handshakePhase = new Phaser(1);

    public ClientSession(Config config, ChannelOut endPoint, Destination dest) {
        super(config, endPoint, dest, "ClientSession localPort=" + endPoint.getLocalPort());
        LOG.info("Created " + toString());
    }

    /**
     * Send connection handshake until a reply from server is received.
     *
     * @return an async event stream.
     */
    public Observable<?> connect() {
        return Observable.create(subscriber -> {
            int n = 0;
            int phase = 0;
            try {
                while (getStatus() != READY && !subscriber.isUnsubscribed()) {
                    try {
                        if (getStatus() == INVALID) {
                            throw new IOException("Can't connect!");
                        }
                        if (getStatus().seqNo() <= HANDSHAKING.seqNo()) {
                            if (getStatus() == START) setStatus(HANDSHAKING);
                            sendInitialHandShake();
                        }
                        else if (getStatus() == HANDSHAKING2) {
                            sendSecondHandshake();
                        }

                        if (getStatus() == INVALID) {
                            throw new IllegalStateException("Can't connect!");
                        }
                        if (n++ > 50) {
                            throw new TimeoutException("Could not connect to server within the timeout.");
                        }
                        else phase = awaitNextHandshakePhase(phase);
                    }
                    catch (IllegalStateException | TimeoutException | IOException ex) {
                        LOG.error("Client connection error", ex);
                        subscriber.onError(ex);
                    }
                }
                if (getStatus() == READY) {
                    cc.init();
                    LOG.info("Connected, {} handshake packets sent", n);
                }
            }
            catch (InterruptedException ex) {
                LOG.info("ClientSession was interrupted while connecting on attempt {}.", n);
            }
            subscriber.onCompleted();
        }).subscribeOn(Schedulers.io());
    }

    private int awaitNextHandshakePhase(final int phase) throws InterruptedException {
        try {
            return handshakePhase.awaitAdvanceInterruptibly(phase, 100, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException ex) {
            // Do nothing.
        }
        return phase;
    }

    @Override
    public boolean receiveHandshake(final Subscriber<? super BoltEvent> subscriber, final ConnectionHandshake handshake,
                                    final Destination peer) {
        boolean readyToStart = false;
        if (getStatus() == HANDSHAKING) {
            LOG.info("Received initial handshake response from {}\n{}", peer, handshake);
            if (handshake.getConnectionType() == ConnectionHandshake.CONNECTION_SERVER_ACK) {
                try {
                    final int peerSocketID = handshake.getSessionID();
                    state.setSessionCookie(handshake.getCookie());
                    state.setDestinationSocketID(peerSocketID);
                    setStatus(HANDSHAKING2);
                    handshakePhase.arrive();
                }
                catch (Exception ex) {
                    LOG.warn("Error creating socket", ex);
                    setStatus(INVALID);
                    subscriber.onError(ex);
                }
            }
            else {
                final Exception ex = new IllegalStateException("Unexpected type of handshake packet received");
                LOG.error("Bad connection type received", ex);
                setStatus(INVALID);
                subscriber.onError(ex);
            }
        }
        else if (getStatus() == HANDSHAKING2) {
            try {
                LOG.info("Received confirmation handshake response from {}\n{}", peer, handshake);
                setStatus(READY);
                handshakePhase.arrive();
                readyToStart = true;
            }
            catch (Exception ex) {
                LOG.error("Error creating socket", ex);
                setStatus(INVALID);
                subscriber.onError(ex);
            }
        }
        return readyToStart;
    }

    /**
     * Initial handshake for connect.
     */
    private void sendInitialHandShake() throws IOException {
        final ConnectionHandshake handshake = ConnectionHandshake.ofClientInitial(getDatagramSize(), state.getInitialSequenceNumber(),
                state.getFlowWindowSize(), state.getSessionID(), endPoint.getLocalAddress());
        LOG.info("Sending {}", handshake);
        endPoint.doSend(handshake, state);
    }

    /**
     * Second handshake for connect.
     */
    private void sendSecondHandshake() throws IOException {
        final ConnectionHandshake ch = ConnectionHandshake.ofClientSecond(getDatagramSize(), state.getInitialSequenceNumber(),
                state.getFlowWindowSize(), getSessionID(), state.getDestinationSessionID(), state.getSessionCookie(), endPoint.getLocalAddress());
        LOG.info("Sending confirmation {}", ch);
        endPoint.doSend(ch, state);
    }

}
