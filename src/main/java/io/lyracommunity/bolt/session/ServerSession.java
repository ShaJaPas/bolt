package io.lyracommunity.bolt.session;

import io.lyracommunity.bolt.Endpoint;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.packet.BoltPacket;
import io.lyracommunity.bolt.packet.ConnectionHandshake;
import io.lyracommunity.bolt.packet.Destination;
import io.lyracommunity.bolt.util.SeqNum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;

import java.io.IOException;
import java.net.InetAddress;
import java.text.MessageFormat;

import static io.lyracommunity.bolt.session.SessionStatus.*;

/**
 * Server side session in client-server mode.
 */
public class ServerSession extends Session {

    private static final Logger LOG = LoggerFactory.getLogger(ServerSession.class);
    private static final String DESCRIPTION_TEMPLATE = "ServerSession localPort={0} peer={1}:{2}";

    private ConnectionHandshake finalConnectionHandshake;

    public ServerSession(final Config config, final Endpoint endPoint, final Destination peer) {
        super(config,
                endPoint, peer, MessageFormat.format(DESCRIPTION_TEMPLATE, endPoint.getLocalPort(), peer.getAddress(), peer.getPort()));
        LOG.info("Created {} talking to {}:{}", toString(), peer.getAddress(), peer.getPort());
    }

    /**
     * Reply to a connection handshake message.
     *
     * @param subscriber the reactive subscriber.
     * @param handshake  incoming connection handshake from the client.
     * @param peer the destination from where the packet was sent.
     */
    @Override
    public boolean receiveHandshake(final Subscriber<? super Object> subscriber, final ConnectionHandshake handshake,
                                    final Destination peer) {
        LOG.info("Received {} in state [{}]", handshake, getStatus());
        boolean readyToStart = false;
        if (getStatus() == READY) {
            // Just send confirmation packet again.
            try {
                sendFinalHandShake(handshake);
            }
            catch (final IOException io) {
                LOG.debug("Sender another confirmation packet. Reason: [{}]", io.getMessage());
            }
        }

        else if (getStatus().seqNo() < READY.seqNo()) {
            state.setDestinationSocketID(handshake.getSocketID());

            if (getStatus().seqNo() < HANDSHAKING.seqNo()) {
                setStatus(HANDSHAKING);
            }

            try {
                boolean handShakeComplete = handleSecondHandShake(handshake);
                if (handShakeComplete) {
                    LOG.info("Client/Server handshake complete!");
                    setStatus(READY);
                    readyToStart = true;
                    cc.init();
                }
            }
            catch (IOException ex) {
                // Session invalid.
                LOG.warn("Error processing ConnectionHandshake", ex);
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
                sender.receive(packet);
                receiver.receive(packet);
            }
            catch (Exception ex) {
                // Invalidate session
                LOG.error("Session error receiving packet", ex);
                setStatus(INVALID);
                subscriber.onError(ex);
            }
        }
    }

    /**
     * Handle the second connection handshake.
     *
     * @param handshake the second connection handshake.
     * @throws IOException if the received cookie doesn't equal the expected cookie.
     */
    private boolean handleSecondHandShake(final ConnectionHandshake handshake) throws IOException {
        if (state.getSessionCookie() == 0) {
            ackInitialHandshake(handshake);
            // Need one more handshake.
            return false;
        }

        final long otherCookie = handshake.getCookie();
        if (state.getSessionCookie() != otherCookie) {
            setStatus(INVALID);
            throw new IOException(MessageFormat.format("Invalid cookie [{0}] received; Expected cookie is [{1}]",
                    otherCookie, state.getSessionCookie()));
        }
        sendFinalHandShake(handshake);
        return true;
    }

    /**
     * Response after the initial connection handshake received:
     * compute cookie
     */
    private void ackInitialHandshake(final ConnectionHandshake handshake) throws IOException, IllegalStateException {
        // Compare the packet size and choose minimum.
        final long clientBufferSize = handshake.getPacketSize();
        final long myBufferSize = getDatagramSize();
        final long bufferSize = Math.min(clientBufferSize, myBufferSize);
        final int initialSequenceNumber = handshake.getInitialSeqNo();
        state.setInitialSequenceNumber(initialSequenceNumber);
        setDatagramSize((int) bufferSize);
        state.setSessionCookie(SeqNum.randomInt());

        final InetAddress localAddress = endPoint.getLocalAddress();
        if (localAddress == null) throw new IllegalStateException("Could not get local endpoint address for handshake response");

        final ConnectionHandshake responseHandshake = ConnectionHandshake.ofServerHandshakeResponse(bufferSize, initialSequenceNumber,
                handshake.getMaxFlowWndSize(), getSocketID(), state.getDestinationSocketID(), state.getSessionCookie(), localAddress);
        LOG.info("Sending reply {}", responseHandshake);
        endPoint.doSend(responseHandshake, state);
    }

    private void sendFinalHandShake(final ConnectionHandshake handshake) throws IOException {

        if (finalConnectionHandshake == null) {
            // Compare the packet size and choose minimum
            final long clientBufferSize = handshake.getPacketSize();
            final long myBufferSize = getDatagramSize();
            final long bufferSize = Math.min(clientBufferSize, myBufferSize);
            final int initialSequenceNumber = handshake.getInitialSeqNo();
            state.setInitialSequenceNumber(initialSequenceNumber);
            setDatagramSize((int) bufferSize);

            finalConnectionHandshake = ConnectionHandshake.ofServerHandshakeResponse(bufferSize, initialSequenceNumber,
                    handshake.getMaxFlowWndSize(), getSocketID(), state.getDestinationSocketID(), state.getSessionCookie(), endPoint.getLocalAddress());
        }
        LOG.info("Sending final handshake ack {}", finalConnectionHandshake);
        endPoint.doSend(finalConnectionHandshake, state);
    }

}

