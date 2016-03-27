package bolt;

import bolt.packet.ConnectionHandshake;
import bolt.packet.Destination;
import bolt.packet.Shutdown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.net.SocketException;

import static bolt.BoltSession.SessionState.*;

/**
 * Client side of a client-server Bolt connection.
 * Once established, the session provides a valid {@link BoltSocket}.
 */
public class ClientSession extends BoltSession {

    private static final Logger LOG = LoggerFactory.getLogger(ClientSession.class);

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
    public Observable<?> connect() throws InterruptedException, IOException {
        return Observable.create(subscriber -> {
            int n = 0;
            while (getState() != READY && !subscriber.isUnsubscribed()) {
                try {
                    if (getState() == INVALID) {
                        throw new IOException("Can't connect!");
                    }
                    if (getState().seqNo() <= HANDSHAKING.seqNo()) {
                        setState(HANDSHAKING);
                        sendInitialHandShake();
                    }
                    else if (getState() == HANDSHAKING2) {
                        sendSecondHandshake();
                    }

                    if (getState() == INVALID) throw new IOException("Can't connect!");
                    if (n++ > 10) throw new IOException("Could not connect to server within the timeout.");

                    Thread.sleep(500);
                }
                catch (IOException | InterruptedException ex) {
                    subscriber.onError(ex);
                }
            }
            cc.init();
            LOG.info("Connected, " + n + " handshake packets sent");
            subscriber.onCompleted();
        }).subscribeOn(Schedulers.io());
    }

    @Override
    public boolean receiveHandshake(final Subscriber<? super Object> subscriber, final ConnectionHandshake handshake,
                                    final Destination peer) {
        if (getState() == HANDSHAKING) {
            LOG.info("Received initial handshake response from " + peer + "\n" + handshake);
            if (handshake.getConnectionType() == ConnectionHandshake.CONNECTION_SERVER_ACK) {
                try {
                    // TODO validate parameters sent by peer
                    int peerSocketID = handshake.getSocketID();
                    sessionCookie = handshake.getCookie();
                    destination.setSocketID(peerSocketID);
                    setState(HANDSHAKING2);
                }
                catch (Exception ex) {
                    LOG.warn("Error creating socket", ex);
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
                LOG.info("Received confirmation handshake response from " + peer + "\n" + handshake);
                // TODO validate parameters sent by peer
                setState(READY);
                socket = new BoltSocket(endPoint, this);
                socket.start().subscribe(subscriber);
            }
            catch (Exception ex) {
                LOG.error("Error creating socket", ex);
                setState(INVALID);
            }
        }
        return isReady();
    }

    @Override
    public void received(BoltPacket packet, Destination peer) {
        if (getState() == READY) {

            if (packet.isControlPacket() && packet instanceof Shutdown) {
                setState(SHUTDOWN);
                active = false;
                LOG.info("Connection shutdown initiated by the other side.");
            }
            else {
                active = true;
                try {
                    // Send all packets to both sender and receiver
                    socket.getSender().receive(packet);
                    socket.getReceiver().receive(packet);
                }
                catch (Exception ex) {
                    // Session is invalid.
                    LOG.error("Error in " + toString(), ex);
                    setState(INVALID);
                }
            }
        }
    }

    /**
     * Initial handshake for connect.
     */
    protected void sendInitialHandShake() throws IOException {
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
        final ConnectionHandshake ch = ConnectionHandshake.ofClientSecond(getDatagramSize(), getInitialSequenceNumber(),
                flowWindowSize, mySocketID, getDestination().getSocketID(), sessionCookie, endPoint.getLocalAddress());
        ch.setSession(this);
        LOG.info("Sending confirmation " + ch);
        endPoint.doSend(ch);
    }


}
