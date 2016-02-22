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

/**
 * server side session in client-server mode
 */
public class ServerSession extends BoltSession {

    private static final Logger logger = Logger.getLogger(ServerSession.class.getName());

    private final BoltEndPoint endPoint;

    private ConnectionHandshake finalConnectionHandshake;

    public ServerSession(Destination peer, BoltEndPoint endPoint) throws SocketException, UnknownHostException {
        super("ServerSession localPort=" + endPoint.getLocalPort() + " peer=" + peer.getAddress() + ":" + peer.getPort(), peer);
        this.endPoint = endPoint;
        logger.info("Created " + toString() + " talking to " + peer.getAddress() + ":" + peer.getPort());
    }

    @Override
    public void received(BoltPacket packet, Destination peer) {
        lastPacket = packet;

        if (packet.isConnectionHandshake()) {
            handleHandShake((ConnectionHandshake) packet);
            return;
        }

        if (packet instanceof KeepAlive) {
            socket.getReceiver().resetEXPTimer();
            active = true;
            return;
        }

        if (packet instanceof Shutdown) {
            try {
                socket.getReceiver().stop();
            } catch (IOException ex) {
                logger.log(Level.WARNING, "", ex);
            }
            setState(shutdown);
            active = false;
            logger.info("Connection shutdown initiated by peer.");
            return;
        }

        if (getState() == ready) {
            active = true;
            try {
                if (packet.forSender()) {
                    socket.getSender().receive(packet);
                } else {
                    socket.getReceiver().receive(packet);
                }
            } catch (Exception ex) {
                //session invalid
                logger.log(Level.SEVERE, "", ex);
                setState(invalid);
            }
            return;
        }

    }

    /**
     * reply to a connection handshake message
     *
     * @param connectionHandshake
     */
    protected void handleHandShake(ConnectionHandshake connectionHandshake) {
        logger.info("Received " + connectionHandshake + " in state <" + getState() + ">");
        if (getState() == ready) {
            //just send confirmation packet again
            try {
                sendFinalHandShake(connectionHandshake);
            } catch (IOException io) {
            }
            return;
        }

        if (getState() < ready) {
            destination.setSocketID(connectionHandshake.getSocketID());

            if (getState() < handshaking) {
                setState(handshaking);
            }

            try {
                boolean handShakeComplete = handleSecondHandShake(connectionHandshake);
                if (handShakeComplete) {
                    logger.info("Client/Server handshake complete!");
                    setState(ready);
                    socket = new BoltSocket(endPoint, this);
                    cc.init();
                }
            } catch (IOException ex) {
                //session invalid
                logger.log(Level.WARNING, "Error processing ConnectionHandshake", ex);
                setState(invalid);
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
            //need one more handshake
            return false;
        }

        long otherCookie = handshake.getCookie();
        if (sessionCookie != otherCookie) {
            setState(invalid);
            throw new IOException("Invalid cookie <" + otherCookie + "> received, my cookie is <" + sessionCookie + ">");
        }
        sendFinalHandShake(handshake);
        return true;
    }

    /**
     * response after the initial connection handshake received:
     * compute cookie
     */
    protected void ackInitialHandshake(ConnectionHandshake handshake) throws IOException {
        ConnectionHandshake responseHandshake = new ConnectionHandshake();
        //compare the packet size and choose minimum
        long clientBufferSize = handshake.getPacketSize();
        long myBufferSize = getDatagramSize();
        long bufferSize = Math.min(clientBufferSize, myBufferSize);
        long initialSequenceNumber = handshake.getInitialSeqNo();
        setInitialSequenceNumber(initialSequenceNumber);
        setDatagramSize((int) bufferSize);
        responseHandshake.setPacketSize(bufferSize);
        responseHandshake.setBoltVersion(4);
        responseHandshake.setInitialSeqNo(initialSequenceNumber);
        responseHandshake.setConnectionType(-1);
        responseHandshake.setMaxFlowWndSize(handshake.getMaxFlowWndSize());
        //tell peer what the socket ID on this side is
        responseHandshake.setSocketID(mySocketID);
        responseHandshake.setDestinationID(this.getDestination().getSocketID());
        responseHandshake.setSession(this);
        sessionCookie = SequenceNumber.random();
        responseHandshake.setCookie(sessionCookie);
        responseHandshake.setAddress(endPoint.getLocalAddress());
        logger.info("Sending reply " + responseHandshake);
        endPoint.doSend(responseHandshake);
    }


    protected void sendFinalHandShake(ConnectionHandshake handshake) throws IOException {

        if (finalConnectionHandshake == null) {
            finalConnectionHandshake = new ConnectionHandshake();
            //compare the packet size and choose minimum
            long clientBufferSize = handshake.getPacketSize();
            long myBufferSize = getDatagramSize();
            long bufferSize = Math.min(clientBufferSize, myBufferSize);
            long initialSequenceNumber = handshake.getInitialSeqNo();
            setInitialSequenceNumber(initialSequenceNumber);
            setDatagramSize((int) bufferSize);
            finalConnectionHandshake.setPacketSize(bufferSize);
            finalConnectionHandshake.setBoltVersion(4);
            finalConnectionHandshake.setInitialSeqNo(initialSequenceNumber);
            finalConnectionHandshake.setConnectionType(-1);
            finalConnectionHandshake.setMaxFlowWndSize(handshake.getMaxFlowWndSize());
            //tell peer what the socket ID on this side is
            finalConnectionHandshake.setSocketID(mySocketID);
            finalConnectionHandshake.setDestinationID(this.getDestination().getSocketID());
            finalConnectionHandshake.setSession(this);
            finalConnectionHandshake.setCookie(sessionCookie);
            finalConnectionHandshake.setAddress(endPoint.getLocalAddress());
        }
        logger.info("Sending final handshake ack " + finalConnectionHandshake);
        endPoint.doSend(finalConnectionHandshake);
    }

}

