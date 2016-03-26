package bolt;

import bolt.packets.ConnectionHandshake;
import bolt.packets.Destination;
import bolt.statistic.BoltStatistics;
import bolt.util.SequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;

import java.net.DatagramPacket;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * <h2>Connection Setup and shutdown</h2>
 * <p>
 * Bolt supports two different connection setup methods, the traditional
 * client/server mode and the rendezvous mode. In the latter mode, both
 * Bolt sockets connect to each other at (approximately) the same time.
 * <p>
 * The Bolt client (in rendezvous mode, both peer are clients) sends a
 * handshake request (type 0 control packet) to the server or the peer
 * side. The handshake packet has the following information (suppose Bolt
 * socket A sends this handshake to B):
 * <ol>
 * <li> Bolt version: this value is for compatibility purpose. The
 * current version is 4.
 * <li> Socket Type: STREAM (0) or DGRAM (1).
 * <li> Initial Sequence Number: It is the sequence number for the first
 * data packet that A will send out. This should be a random value.
 * <li> Packet Size: the maximum size of a data packet (including all
 * headers). This is usually the value of MTU.
 * <li> Maximum Flow Window Size: This value may not be necessary;
 * however, it is needed in the current reference implementation.
 * <li> Connection Type. This information is used to differential the
 * connection setup modes and request/response.
 * <li> Socket ID. The client Bolt socket ID.
 * <li> Cookie. This is a cookie value used to avoid SYN flooding attack
 * [RFC4987].
 * <li> Peer IP address: B's IP address.
 * </ol>
 * <p>
 * <h3>Client/Server Connection Setup</h3>
 * <p>
 * One Bolt entity starts first as the server (listener). The server
 * accepts and processes incoming connection request, and creates new
 * Bolt socket for each new connection.
 * <p>
 * A client that wants to connect to the server will send a handshake
 * packet first. The client should keep on sending the handshake packet
 * every constant interval until it receives a response handshake from
 * the server or a timeout timer expires.
 * <p>
 * When the server first receives the connection request from a client,
 * it generates a cookie value according to the client address and a
 * secret key and sends it back to the client. The client must then send
 * back the same cookie to the server.
 * <p>
 * The server, when receiving a handshake packet and the correct cookie,
 * compares the packet size and maximum window size with its own values
 * and set its own values as the smaller ones. The result values are
 * also sent back to the client by a response handshake packet, together
 * with the server's version and initial sequence number. The server is
 * ready for sending/receiving data right after this step is finished.
 * However, it must send back response packet as long as it receives any
 * further handshakes from the same client.
 * <p>
 * The client can start sending/receiving data once it gets a response
 * handshake packet from the server. Further response handshake
 * messages, if received any, should be omitted.
 * <p>
 * The connection type from the client should be set to 1 and the
 * response from the server should be set to -1.
 * <p>
 * The client should also check if the response is from the server that
 * the original request was sent to.
 * <h3>Rendezvous Connection Setup</h3>
 * <p>
 * In this mode, both clients send a connect request to each other at
 * the same time. The initial connection type is set to 0. Once a peer
 * receives a connection request, it sends back a response. If the
 * connection type is 0, then the response sends back -1; if the
 * connection type is -1, then the response sends back -2; No response
 * will be sent for -2 request.
 * <p>
 * The rendezvous peer does the same check on the handshake messages
 * (version, packet size, window size, etc.) as described in Section
 * 5.1. In addition, the peer only process the connection request from
 * the address it has sent a connection request to. Finally, rendezvous
 * connection should be rejected by a regular Bolt server (listener).
 * A peer initializes the connection when it receives -1 response.
 * <p>
 * The rendezvous connection setup is useful when both peers are behind
 * firewalls. It can also provide better security and usability when a
 * listening server is not desirable.
 * <h3>Shutdown</h3>
 * <p>
 * If one of the connected Bolt entities is being closed, it will send a
 * shutdown message to the peer side. The peer side, after received this
 * message, will also be closed. This shutdown message, delivered using
 * UDP, is only sent once and not guaranteed to be received. If the
 * message is not received, the peer side will be closed after 16
 * continuous EXP timeout (see section 3.5). However, the total timeout
 * value should be between a minimum threshold and a maximum threshold.
 * In our reference implementation, we use 3 seconds and 30 seconds,
 * respectively.
 */
public abstract class BoltSession {


    private static final Logger LOG = LoggerFactory.getLogger(BoltSession.class);

    private final static AtomicInteger NEXT_SOCKET_ID = new AtomicInteger(20 + new Random().nextInt(5000));
    protected final BoltStatistics statistics;
    protected final CongestionControl cc;
    /**
     * remote Bolt entity (address and socket ID)
     */
    protected final Destination destination;
    protected final int mySocketID;
    protected volatile boolean active;
    protected volatile BoltSocket socket;
    protected int receiveBufferSize = 64 * 32768;
    /**
     * Session cookie created during handshake.
     */
    protected long sessionCookie = 0;

    /**
     * Flow window size (how many data packets are in-flight at a single time).
     */
    protected int flowWindowSize = 1024 * 10;
    /**
     * Buffer size (i.e. datagram size). This is negotiated during connection setup.
     */
    protected int datagramSize = BoltEndPoint.DATAGRAM_SIZE;
    protected Integer initialSequenceNumber = null;
    private volatile SessionState state = SessionState.START;

    // Cache dgPacket (peer stays the same always)
    private DatagramPacket dgPacket;

    public BoltSession(final String description, final Destination destination) {
        this.statistics = new BoltStatistics(description, datagramSize);
        this.mySocketID = NEXT_SOCKET_ID.incrementAndGet();
        this.destination = destination;
        this.dgPacket = new DatagramPacket(new byte[0], 0, destination.getAddress(), destination.getPort());
        this.cc = new BoltCongestionControl(this);
    }

    public abstract void received(BoltPacket packet, Destination peer);

    public abstract boolean receiveHandshake(Subscriber<? super Object> subscriber, ConnectionHandshake handshake, Destination peer);

    public BoltSocket getSocket() {
        return socket;
    }

    public void setSocket(BoltSocket socket) {
        this.socket = socket;
    }

    public CongestionControl getCongestionControl() {
        return cc;
    }

    public SessionState getState() {
        return state;
    }

    public void setState(final SessionState state) {
        LOG.info(toString() + " connection state CHANGED to <" + state + ">");
        this.state = state;
    }

    public boolean isReady() {
        return state == SessionState.READY;
    }

    public boolean isActive() {
        return active;
    }

    public boolean isShutdown() {
        return state == SessionState.SHUTDOWN || state == SessionState.INVALID;
    }

    public Destination getDestination() {
        return destination;
    }

    public int getDatagramSize() {
        return datagramSize;
    }

    public void setDatagramSize(int datagramSize) {
        this.datagramSize = datagramSize;
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public int getFlowWindowSize() {
        return flowWindowSize;
    }

    public BoltStatistics getStatistics() {
        return statistics;
    }

    public long getSocketID() {
        return mySocketID;
    }

    public synchronized int getInitialSequenceNumber() {
        if (initialSequenceNumber == null) {
            initialSequenceNumber = SequenceNumber.randomPacketSeqNum();
        }
        return initialSequenceNumber;
    }

    public synchronized void setInitialSequenceNumber(int initialSequenceNumber) {
        this.initialSequenceNumber = initialSequenceNumber;
    }

    public DatagramPacket getDatagram() {
        return dgPacket;
    }

    public String toString() {
        return super.toString() +
                " [" +
                "socketID=" + this.mySocketID +
                " ]";
    }

    public enum SessionState {
        START(0),
        HANDSHAKING(1),
        HANDSHAKING2(2),
        READY(50),
        KEEPALIVE(80),
        SHUTDOWN(90),
        INVALID(99);

        final int seqNo;

        SessionState(final int seqNo) {
            this.seqNo = seqNo;
        }

        public int seqNo() {
            return seqNo;
        }
    }

}
