package bolt;

import bolt.packets.ConnectionHandshake;
import bolt.packets.Destination;
import bolt.packets.Shutdown;

import java.io.IOException;
import java.net.SocketException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static bolt.BoltSession.SessionState.*;

/**
 * Client side of a client-server Bolt connection.
 * Once established, the session provides a valid {@link BoltSocket}.
 */
public class ClientSession extends BoltSession {

    private static final Logger LOG = Logger.getLogger(ClientSession.class.getName());

    private final BoltEndPoint endPoint;


    public ClientSession(BoltEndPoint endPoint, Destination dest) throws SocketException {
        super("ClientSession localPort=" + endPoint.getLocalPort(), dest);
        this.endPoint = endPoint;
        LOG.info("Created " + toString());
    }

    /**
     * send connection handshake until a reply from server is received
     *
     * @throws InterruptedException
     * @throws IOException
     */

    public void connect() throws InterruptedException, IOException {
        int n = 0;
        while (getState() != READY) {
            if (getState() == INVALID) throw new IOException("Can't connect!");
            if (getState().seqNo() <= HANDSHAKING.seqNo()) {
                setState(HANDSHAKING);
                sendInitialHandShake();
            } else if (getState() == HANDSHAKING2) {
                sendSecondHandshake();
            }

            if (getState() == INVALID) throw new IOException("Can't connect!");
            if (n++ > 10) throw new IOException("Could not connect to server within the timeout.");

            Thread.sleep(500);
        }
        Thread.sleep(1000);
        cc.init();
        LOG.info("Connected, " + n + " handshake packets sent");
    }

    @Override
    public void received(BoltPacket packet, Destination peer) {

        if (packet.isConnectionHandshake()) {
            ConnectionHandshake hs = (ConnectionHandshake) packet;
            handleConnectionHandshake(hs, peer);
            return;
        }

        if (getState() == READY) {

            if (packet instanceof Shutdown) {
                setState(SHUTDOWN);
                active = false;
                LOG.info("Connection shutdown initiated by the other side.");
                return;
            }
            active = true;
            try {
                if (packet.forSender()) {
                    socket.getSender().receive(packet);
                } else {
                    socket.getReceiver().receive(packet);
                }
            } catch (Exception ex) {
                //session is invalid
                LOG.log(Level.SEVERE, "Error in " + toString(), ex);
                setState(INVALID);
            }
        }
    }

    protected void handleConnectionHandshake(ConnectionHandshake hs, Destination peer) {

        if (getState() == HANDSHAKING) {
            LOG.info("Received initial handshake response from " + peer + "\n" + hs);
            if (hs.getConnectionType() == ConnectionHandshake.CONNECTION_SERVER_ACK) {
                try {
                    //TODO validate parameters sent by peer
                    long peerSocketID = hs.getSocketID();
                    sessionCookie = hs.getCookie();
                    destination.setSocketID(peerSocketID);
                    setState(HANDSHAKING2);
                }
                catch (Exception ex) {
                    LOG.log(Level.WARNING, "Error creating socket", ex);
                    setState(INVALID);
                }
            }
            else {
                LOG.info("Unexpected type of handshake packet received");
                setState(INVALID);
            }
        }
        else if (getState() == HANDSHAKING2) {
            try {
                LOG.info("Received confirmation handshake response from " + peer + "\n" + hs);
                //TODO validate parameters sent by peer
                setState(READY);
                socket = new BoltSocket(endPoint, this);
            }
            catch (Exception ex) {
                LOG.log(Level.WARNING, "Error creating socket", ex);
                setState(INVALID);
            }
        }
    }

    /**
     * Initial handshake for connect.
     */
    protected void sendInitialHandShake() throws IOException {
//        long initialSequenceNo = SequenceNumber.random();
//        setInitialSequenceNumber(initialSequenceNo);
//        handshake.setInitialSeqNo(initialSequenceNo);
        //TODO CIAN: REMOVED

        final ConnectionHandshake handshake = ConnectionHandshake.ofClientInitial(getDatagramSize(), getInitialSequenceNumber(),
                flowWindowSize, mySocketID, endPoint.getLocalAddress());
        handshake.setSession(this);
        LOG.info("Sending " + handshake);
        endPoint.doSend(handshake);
    }

    /**
     * Second handshake for connect.
     */
    protected void sendSecondHandshake() throws IOException {
        ConnectionHandshake handshake = new ConnectionHandshake();
        handshake.setConnectionType(ConnectionHandshake.CONNECTION_TYPE_REGULAR);
        handshake.setSocketType(ConnectionHandshake.SOCKET_TYPE_DGRAM);

        //TODO CIAN: REMOVED
//        handshake.setInitialSeqNo(initialSequenceNo);
        handshake.setInitialSeqNo(getInitialSequenceNumber());


        handshake.setPacketSize(getDatagramSize());
        handshake.setSocketID(mySocketID);
        handshake.setMaxFlowWndSize(flowWindowSize);
        handshake.setSession(this);
        handshake.setCookie(sessionCookie);
        handshake.setAddress(endPoint.getLocalAddress());
        handshake.setDestinationID(getDestination().getSocketID());
        LOG.info("Sending confirmation " + handshake);
        endPoint.doSend(handshake);
    }


}
