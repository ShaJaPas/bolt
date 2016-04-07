package io.lyracommunity.bolt.session;

import io.lyracommunity.bolt.BoltEndPoint;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.packet.BoltPacket;
import io.lyracommunity.bolt.packet.ConnectionHandshake;
import io.lyracommunity.bolt.packet.Destination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.net.SocketException;

import static io.lyracommunity.bolt.session.SessionStatus.*;

/**
 * Client side of a client-server Bolt connection.
 */
public class ClientSession extends Session {

    private static final Logger LOG = LoggerFactory.getLogger(ClientSession.class);


    public ClientSession(Config config, BoltEndPoint endPoint, Destination dest) throws SocketException {
        super(config, endPoint, dest, "ClientSession localPort=" + endPoint.getLocalPort());
        LOG.info("Created " + toString());
    }

    /**
     * Send connection handshake until a reply from server is received.
     *
     * @throws InterruptedException
     * @throws IOException
     */
    public Observable<?> connect() throws InterruptedException, IOException {
        return Observable.create(subscriber -> {
            int n = 0;
            while (getStatus() != READY && !subscriber.isUnsubscribed()) {
                try {
                    if (getStatus() == INVALID) {
                        throw new IOException("Can't connect!");
                    }
                    if (getStatus().seqNo() <= HANDSHAKING.seqNo()) {
                        setStatus(HANDSHAKING);
                        sendInitialHandShake();
                    }
                    else if (getStatus() == HANDSHAKING2) {
                        sendSecondHandshake();
                    }

                    if (getStatus() == INVALID) throw new IOException("Can't connect!");
                    if (n++ > 40) throw new IOException("Could not connect to server within the timeout.");

                    Thread.sleep(500);
                }
                catch (InterruptedException ex) {
                    // Do nothing.
                }
                catch (IOException ex) {
                    subscriber.onError(ex);
                }
            }
            cc.init();
            LOG.info("Connected, {} handshake packets sent", n);
            subscriber.onCompleted();
        }).subscribeOn(Schedulers.io());
    }

    @Override
    public boolean receiveHandshake(final Subscriber<? super Object> subscriber, final ConnectionHandshake handshake,
                                    final Destination peer) {
        boolean readyToStart = false;
        if (getStatus() == HANDSHAKING) {
            LOG.info("Received initial handshake response from {}\n{}", peer, handshake);
            if (handshake.getConnectionType() == ConnectionHandshake.CONNECTION_SERVER_ACK) {
                try {
                    // TODO validate parameters sent by peer
                    int peerSocketID = handshake.getSocketID();
                    state.setSessionCookie(handshake.getCookie());
                    state.getDestination().setSocketID(peerSocketID);
                    setStatus(HANDSHAKING2);
//                    sendSecondHandshake();
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
                // TODO validate parameters sent by peer
                setStatus(READY);
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

    @Override
    public void received(final BoltPacket packet, final Subscriber subscriber) {
        if (getStatus() == READY) {
            state.setActive(true);
            try {
                // Send all packets to both sender and receiver
                sender.receive(packet);
                receiver.receive(packet);
            }
            catch (Exception ex) {
                // Session is invalid.
                LOG.error("Error in " + toString(), ex);
                subscriber.onError(ex);
                setStatus(INVALID);
            }
        }
    }

    /**
     * Initial handshake for connect.
     */
    protected void sendInitialHandShake() throws IOException {
        final ConnectionHandshake handshake = ConnectionHandshake.ofClientInitial(state.getDatagramSize(), state.getInitialSequenceNumber(),
                state.getFlowWindowSize(), state.getSocketID(), endPoint.getLocalAddress());
        LOG.info("Sending {}", handshake);
        endPoint.doSend(handshake, state);
    }

    /**
     * Second handshake for connect.
     */
    protected void sendSecondHandshake() throws IOException {
        final ConnectionHandshake ch = ConnectionHandshake.ofClientSecond(state.getDatagramSize(), state.getInitialSequenceNumber(),
                state.getFlowWindowSize(), getSocketID(), state.getDestinationSocketID(), state.getSessionCookie(), endPoint.getLocalAddress());
        LOG.info("Sending confirmation {}", ch);
        endPoint.doSend(ch, state);
    }

}
