package io.lyracommunity.bolt.session;

import io.lyracommunity.bolt.ChannelOut;
import io.lyracommunity.bolt.api.BoltEvent;
import io.lyracommunity.bolt.api.Config;
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

    private static final Logger LOG                  = LoggerFactory.getLogger(ServerSession.class);
    private static final String DESCRIPTION_TEMPLATE = "ServerSession localPort={0} peer={1}:{2}";

    private volatile ConnectionHandshake finalConnectionHandshake;

    public ServerSession(final Config config, final ChannelOut endPoint, final Destination peer) {
        super(config,
                endPoint, peer, MessageFormat.format(DESCRIPTION_TEMPLATE, endPoint.getLocalPort(), peer.getAddress(), peer.getPort()));
        LOG.info("Created {} talking to {}:{}", toString(), peer.getAddress(), peer.getPort());
    }

    /**
     * Reply to a connection handshake message.
     *
     * @param subscriber the reactive subscriber.
     * @param handshake  incoming connection handshake from the client.
     * @param peer       the destination from where the packet was sent.
     */
    @Override
    public boolean receiveHandshake(final Subscriber<? super BoltEvent> subscriber, final ConnectionHandshake handshake,
                                    final Destination peer) {
        LOG.info("Handshake received in state [{}]:  {}", getStatus(), handshake);
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

            if (getStatus().seqNo() < HANDSHAKING.seqNo()) {
                setStatus(HANDSHAKING);
            }
            try {
                final boolean handshakingComplete = handleClientHandshake(handshake);
                if (handshakingComplete) {
                    LOG.info("Handshake complete for Server!  [{}]", getSessionID());
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

    /**
     * Handle the client connection handshake.
     *
     * @param handshake the second connection handshake.
     * @throws IOException if the received cookie doesn't equal the expected cookie.
     */
    private boolean handleClientHandshake(final ConnectionHandshake handshake) throws IOException {
        if (state.getSessionCookie() == 0) {
            state.setSessionCookie(1L + SeqNum.randomInt());
            ackInitialHandshake(handshake);
            // Need one more handshake.
            return false;
        }
        final long otherCookie = handshake.getCookie();
        final boolean initialHandshake = (otherCookie == 0);

        // Is initial, need one more handshake.
        if (initialHandshake) {
            ackInitialHandshake(handshake);
            return false;
        }
        // The cookie is incorrect.
        else if (state.getSessionCookie() != otherCookie) {
            setStatus(INVALID);
            throw new IOException(MessageFormat.format("Invalid cookie [{0}] received; Expected cookie is [{1}]",
                    otherCookie, state.getSessionCookie()));
        }
        // Cookie is confirmed by client and equals server cookie. Send final confirmation handshake.
        else {
            sendFinalHandShake(handshake);
            return true;
        }
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

        final InetAddress localAddress = endPoint.getLocalAddress();
        if (localAddress == null)
            throw new IllegalStateException("Could not get local endpoint address for handshake response");

        final ConnectionHandshake responseHandshake = ConnectionHandshake.ofServerFirstCookieShareResponse(bufferSize, initialSequenceNumber,
                handshake.getMaxFlowWndSize(), getSessionID(), state.getDestinationSessionID(), state.getSessionCookie(), localAddress);
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

            finalConnectionHandshake = ConnectionHandshake.ofServerFinalResponse(bufferSize, initialSequenceNumber,
                    handshake.getMaxFlowWndSize(), getSessionID(), state.getDestinationSessionID(), state.getSessionCookie(), endPoint.getLocalAddress());
        }
        LOG.info("Sending final handshake ack {}", finalConnectionHandshake);
        endPoint.doSend(finalConnectionHandshake, state);
    }

}

