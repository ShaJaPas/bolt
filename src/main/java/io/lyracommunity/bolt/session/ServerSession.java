package io.lyracommunity.bolt.session;

import io.lyracommunity.bolt.BoltEndPoint;
import io.lyracommunity.bolt.packet.BoltPacket;
import io.lyracommunity.bolt.packet.ConnectionHandshake;
import io.lyracommunity.bolt.packet.Destination;
import io.lyracommunity.bolt.util.SeqNum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;

import java.io.IOException;
import java.text.MessageFormat;

import static io.lyracommunity.bolt.session.BoltSession.SessionState.*;

/**
 * Server side session in client-server mode.
 */
public class ServerSession extends BoltSession {

    private static final Logger LOG = LoggerFactory.getLogger(ServerSession.class);
    private static final String DESCRIPTION_TEMPLATE = "ServerSession localPort={0} peer={1}:{2}";

    private ConnectionHandshake finalConnectionHandshake;

    public ServerSession(final Destination peer, final BoltEndPoint endPoint) {
        super(endPoint, MessageFormat.format(DESCRIPTION_TEMPLATE, endPoint.getLocalPort(), peer.getAddress(), peer.getPort()), peer);
        LOG.info("Created {} talking to {}:{}", toString(), peer.getAddress(), peer.getPort());
    }

    /**
     * Reply to a connection handshake message.
     *
     * @param subscriber
     * @param handshake  incoming connection handshake from the client.
     * @param peer
     */
    @Override
    public boolean receiveHandshake(final Subscriber<? super Object> subscriber, final ConnectionHandshake handshake,
                                    final Destination peer) {
        LOG.info("Received {} in state [{}]", handshake, getState());
        boolean readyToStart = false;
        if (getState() == READY) {
            // Just send confirmation packet again.
            try {
                sendFinalHandShake(handshake);
            }
            catch (final IOException io) {
                LOG.debug("Sender another confirmation packet. Reason: [{}]", io.getMessage());
            }
        }

        else if (getState().seqNo() < READY.seqNo()) {
            destination.setSocketID(handshake.getSocketID());

            if (getState().seqNo() < HANDSHAKING.seqNo()) {
                setState(HANDSHAKING);
            }

            try {
                boolean handShakeComplete = handleSecondHandShake(handshake);
                if (handShakeComplete) {
                    LOG.info("Client/Server handshake complete!");
                    setState(READY);
                    readyToStart = true;
                    cc.init();
                }
            }
            catch (IOException ex) {
                // Session invalid.
                LOG.warn("Error processing ConnectionHandshake", ex);
                setState(INVALID);
                subscriber.onError(ex);
            }
        }
        return readyToStart;
    }

    @Override
    public void received(final BoltPacket packet, final Subscriber subscriber) {

        if (getState() == READY) {
            socket.setActive(true);
            try {
                socket.getSender().receive(packet);
                socket.getReceiver().receive(packet);
            }
            catch (Exception ex) {
                // Invalidate session
                LOG.error("Session error receiving packet", ex);
                setState(INVALID);
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
        if (sessionCookie == 0) {
            ackInitialHandshake(handshake);
            // Need one more handshake.
            return false;
        }

        long otherCookie = handshake.getCookie();
        if (sessionCookie != otherCookie) {
            setState(INVALID);
            throw new IOException(MessageFormat.format("Invalid cookie [{0}] received; Expected cookie is [{1}]", otherCookie, sessionCookie));
        }
        sendFinalHandShake(handshake);
        return true;
    }

    /**
     * Response after the initial connection handshake received:
     * compute cookie
     */
    private void ackInitialHandshake(final ConnectionHandshake handshake) throws IOException {
        // Compare the packet size and choose minimum.
        final long clientBufferSize = handshake.getPacketSize();
        final long myBufferSize = getDatagramSize();
        final long bufferSize = Math.min(clientBufferSize, myBufferSize);
        final int initialSequenceNumber = handshake.getInitialSeqNo();
        setInitialSequenceNumber(initialSequenceNumber);
        setDatagramSize((int) bufferSize);
        sessionCookie = SeqNum.randomInt();

        final ConnectionHandshake responseHandshake = ConnectionHandshake.ofServerHandshakeResponse(bufferSize, initialSequenceNumber,
                handshake.getMaxFlowWndSize(), mySocketID, getDestination().getSocketID(), sessionCookie, endPoint.getLocalAddress());
        LOG.info("Sending reply {}", responseHandshake);
        endPoint.doSend(responseHandshake, this);
    }

    private void sendFinalHandShake(final ConnectionHandshake handshake) throws IOException {

        if (finalConnectionHandshake == null) {
            // Compare the packet size and choose minimum
            long clientBufferSize = handshake.getPacketSize();
            long myBufferSize = getDatagramSize();
            long bufferSize = Math.min(clientBufferSize, myBufferSize);
            int initialSequenceNumber = handshake.getInitialSeqNo();
            setInitialSequenceNumber(initialSequenceNumber);
            setDatagramSize((int) bufferSize);

            finalConnectionHandshake = ConnectionHandshake.ofServerHandshakeResponse(bufferSize, initialSequenceNumber,
                    handshake.getMaxFlowWndSize(), mySocketID, getDestination().getSocketID(), sessionCookie, endPoint.getLocalAddress());
        }
        LOG.info("Sending final handshake ack {}", finalConnectionHandshake);
        endPoint.doSend(finalConnectionHandshake, this);
    }

}

