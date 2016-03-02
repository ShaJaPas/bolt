package bolt;

import bolt.packets.ConnectionHandshake;
import bolt.packets.Destination;
import bolt.packets.KeepAlive;
import bolt.packets.Shutdown;
import bolt.util.SequenceNumber;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static bolt.BoltSession.SessionState.*;

/**
 * server side session in client-server mode
 */
public class ServerSession extends BoltSession {

    private static final Logger LOG = Logger.getLogger(ServerSession.class.getName());

    private final BoltEndPoint endPoint;

    private ConnectionHandshake finalConnectionHandshake;

    public ServerSession(Destination peer, BoltEndPoint endPoint) throws SocketException, UnknownHostException {
        super("ServerSession localPort=" + endPoint.getLocalPort() + " peer=" + peer.getAddress() + ":" + peer.getPort(), peer);
        this.endPoint = endPoint;
        LOG.info("Created " + toString() + " talking to " + peer.getAddress() + ":" + peer.getPort());
    }

    @Override
    public void received(final BoltPacket packet, final Destination peer) {

        if (packet.isConnectionHandshake()) {
            handleHandShake((ConnectionHandshake) packet);
        }

        else if (packet instanceof KeepAlive) {
            socket.getReceiver().resetEXPTimer();
            active = true;
        }

        else if (packet instanceof Shutdown) {
            try {
                socket.getReceiver().stop();
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "", ex);
            }
            setState(SHUTDOWN);
            active = false;
            LOG.info("Connection shutdown initiated by peer.");
        }

        else if (getState() == READY) {
            active = true;
            try {
                if (packet.forSender()) {
                    socket.getSender().receive(packet);
                } else {
                    socket.getReceiver().receive(packet);
                }
            } catch (Exception ex) {
                //session invalid
                LOG.log(Level.SEVERE, "", ex);
                setState(INVALID);
            }
        }

    }

    /**
     * Reply to a connection handshake message.
     *
     * @param connectionHandshake incoming connection handshake from the client.
     */
    protected void handleHandShake(ConnectionHandshake connectionHandshake) {
        LOG.info("Received " + connectionHandshake + " in state <" + getState() + ">");
        if (getState() == READY) {
            // Just send confirmation packet again.
            try {
                sendFinalHandShake(connectionHandshake);
            }
            catch (IOException io) {
            }
        }

        else if (getState().seqNo() < READY.seqNo()) {
            destination.setSocketID(connectionHandshake.getSocketID());

            if (getState().seqNo() < HANDSHAKING.seqNo()) {
                setState(HANDSHAKING);
            }

            try {
                boolean handShakeComplete = handleSecondHandShake(connectionHandshake);
                if (handShakeComplete) {
                    LOG.info("Client/Server handshake complete!");
                    setState(READY);
                    socket = new BoltSocket(endPoint, this);
                    cc.init();
                }
            } catch (IOException ex) {
                // Session invalid.
                LOG.log(Level.WARNING, "Error processing ConnectionHandshake", ex);
                setState(INVALID);
            }
        }
    }

    /**
     * handle the connection handshake
     *
     * @param handshake
     * @throws IOException
     */
    protected boolean handleSecondHandShake(ConnectionHandshake handshake) throws IOException {
        if (sessionCookie == 0) {
            ackInitialHandshake(handshake);
            // Need one more handshake.
            return false;
        }

        long otherCookie = handshake.getCookie();
        if (sessionCookie != otherCookie) {
            setState(INVALID);
            throw new IOException("Invalid cookie <" + otherCookie + "> received, my cookie is <" + sessionCookie + ">");
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
        sessionCookie = SequenceNumber.random();

        final ConnectionHandshake responseHandshake = ConnectionHandshake.ofServerHandshakeResponse(bufferSize, initialSequenceNumber,
                handshake.getMaxFlowWndSize(), mySocketID, getDestination().getSocketID(), sessionCookie, endPoint.getLocalAddress());
        responseHandshake.setSession(this);
        LOG.info("Sending reply " + responseHandshake);
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
        LOG.info("Sending final handshake ack " + finalConnectionHandshake);
        endPoint.doSend(finalConnectionHandshake);
    }

}

