package bolt;

import bolt.packets.ConnectionHandshake;
import bolt.packets.Destination;
import bolt.packets.KeepAlive;
import bolt.packets.Shutdown;
import bolt.util.SequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.text.MessageFormat;

import static bolt.BoltSession.SessionState.*;

/**
 * Server side session in client-server mode.
 */
public class ServerSession extends BoltSession {

    private static final Logger LOG = LoggerFactory.getLogger(ServerSession.class);

    private final BoltEndPoint endPoint;

    private ConnectionHandshake finalConnectionHandshake;

    public ServerSession(final Destination peer, final BoltEndPoint endPoint) throws SocketException, UnknownHostException {
        super("ServerSession localPort=" + endPoint.getLocalPort() + " peer=" + peer.getAddress() + ":" + peer.getPort(), peer);
        this.endPoint = endPoint;
        LOG.info("Created {} talking to {}:{}", toString(), peer.getAddress(), peer.getPort());
    }

    /**
     * Reply to a connection handshake message.
     *
     * @param subscriber
     * @param handshake incoming connection handshake from the client.
     * @param peer
     */
    @Override
    public boolean receiveHandshake(final Subscriber<? super Object> subscriber, final ConnectionHandshake handshake,
                                    final Destination peer) {
        LOG.info("Received {} in state [{}]", handshake, getState());
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
                    socket = new BoltSocket(endPoint, this);
                    socket.start().subscribe(subscriber);
                    cc.init();
                }
            }
            catch (IOException ex) {
                // Session invalid.
                LOG.warn("Error processing ConnectionHandshake", ex);
                setState(INVALID);
            }
        }
        return isReady();
    }

    @Override
    public void received(final BoltPacket packet, final Destination peer) {

        if (packet instanceof KeepAlive) {
            socket.getReceiver().resetEXPTimer();
            active = true;
        }

        else if (packet instanceof Shutdown) {
            socket.getReceiver().stop();
            setState(SHUTDOWN);
            active = false;
            LOG.info("Connection shutdown initiated by peer.");
        }

        else if (getState() == READY) {
            active = true;
            try {
                if (packet.forSender()) {
                    socket.getSender().receive(packet);
                }
                else {
                    socket.getReceiver().receive(packet);
                }
            }
            catch (Exception ex) {
                // Invalidate session
                LOG.error("Session error receiving packet", ex);
                setState(INVALID);
            }
        }
    }

    /**
     * Handle the second connection handshake.
     *
     * @param handshake the second connection handshake.
     * @throws IOException if the received cookie doesn't equal the expected cookie.
     */
    protected boolean handleSecondHandShake(final ConnectionHandshake handshake) throws IOException {
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
    protected void ackInitialHandshake(final ConnectionHandshake handshake) throws IOException {
        // Compare the packet size and choose minimum.
        final long clientBufferSize = handshake.getPacketSize();
        final long myBufferSize = getDatagramSize();
        final long bufferSize = Math.min(clientBufferSize, myBufferSize);
        final int initialSequenceNumber = handshake.getInitialSeqNo();
        setInitialSequenceNumber(initialSequenceNumber);
        setDatagramSize((int) bufferSize);
        sessionCookie = SequenceNumber.random(); // TODO use long generation method?

        final ConnectionHandshake responseHandshake = ConnectionHandshake.ofServerHandshakeResponse(bufferSize, initialSequenceNumber,
                handshake.getMaxFlowWndSize(), mySocketID, getDestination().getSocketID(), sessionCookie, endPoint.getLocalAddress());
        responseHandshake.setSession(this);
        LOG.info("Sending reply {}", responseHandshake);
        endPoint.doSend(responseHandshake);
    }

    protected void sendFinalHandShake(ConnectionHandshake handshake) throws IOException {

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
            finalConnectionHandshake.setSession(this);
        }
        LOG.info("Sending final handshake ack {}", finalConnectionHandshake);
        endPoint.doSend(finalConnectionHandshake);
    }

}

