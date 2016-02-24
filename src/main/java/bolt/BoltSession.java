package bolt;

import bolt.packets.Destination;
import bolt.statistic.BoltStatistics;
import bolt.util.SequenceNumber;

import java.net.DatagramPacket;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <h2>Connection Setup and shutdown</h2>
 * <p>
 * UDT supports two different connection setup methods, the traditional
 * client/server mode and the rendezvous mode. In the latter mode, both
 * UDT sockets connect to each other at (approximately) the same time.
 * <p>
 * The UDT client (in rendezvous mode, both peer are clients) sends a
 * handshake request (type 0 control packet) to the server or the peer
 * side. The handshake packet has the following information (suppose UDT
 * socket A sends this handshake to B):
 * <ol>
 * <li> UDT version: this value is for compatibility purpose. The
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
 * <li> Socket ID. The client UDT socket ID.
 * <li> Cookie. This is a cookie value used to avoid SYN flooding attack
 * [RFC4987].
 * <li> Peer IP address: B's IP address.
 * </ol>
 * <p>
 * <h3>Client/Server Connection Setup</h3>
 * <p>
 * One UDT entity starts first as the server (listener). The server
 * accepts and processes incoming connection request, and creates new
 * UDT socket for each new connection.
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
 * connection should be rejected by a regular UDT server (listener).
 * A peer initializes the connection when it receives -1 response.
 * <p>
 * The rendezvous connection setup is useful when both peers are behind
 * firewalls. It can also provide better security and usability when a
 * listening server is not desirable.
 * <h3>Shutdown</h3>
 * <p>
 * If one of the connected UDT entities is being closed, it will send a
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

    //state constants
    public static final int start = 0;
    public static final int handshaking = 1;
    public static final int ready = 50;
    public static final int keepalive = 80;
    public static final int shutdown = 90;
    public static final int invalid = 99;
    public static final int DEFAULT_DATAGRAM_SIZE = BoltEndPoint.DATAGRAM_SIZE;

    /**
     * Key for a system property defining the CC class to be used.
     *
     * @see CongestionControl
     */
    public static final String CC_CLASS = "bolt.congestioncontrol.class";
    private static final Logger logger = Logger.getLogger(BoltSession.class.getName());
    private final static AtomicLong nextSocketID = new AtomicLong(20 + new Random().nextInt(5000));
    protected final BoltStatistics statistics;
    protected final CongestionControl cc;
    /**
     * remote Bolt entity (address and socket ID)
     */
    protected final Destination destination;
    protected final long mySocketID;
    protected int mode;
    protected volatile boolean active;
    protected volatile BoltPacket lastPacket;
    protected volatile BoltSocket socket;
    protected int receiveBufferSize = 64 * 32768;
    //session cookie created during handshake
    protected long sessionCookie = 0;
    /**
     * flow window size, i.e. how many data packets are
     * in-flight at a single time
     */
    protected int flowWindowSize = 1024 * 10;
    /**
     * local port
     */
    protected int localPort;
    /**
     * Buffer size (i.e. datagram size)
     * This is negotiated during connection setup
     */
    protected int datagramSize = DEFAULT_DATAGRAM_SIZE;

    protected Long initialSequenceNumber = null;
    private volatile int state = start;
    //cache dgPacket (peer stays the same always)
    private DatagramPacket dgPacket;

    public BoltSession(String description, Destination destination) {
        statistics = new BoltStatistics(description);
        mySocketID = nextSocketID.incrementAndGet();
        this.destination = destination;
        this.dgPacket = new DatagramPacket(new byte[0], 0, destination.getAddress(), destination.getPort());
        String clazzP = System.getProperty(CC_CLASS, BoltCongestionControl.class.getName());
        Object ccObject;
        try {
            Class<?> clazz = Class.forName(clazzP);
            ccObject = clazz.getDeclaredConstructor(BoltSession.class).newInstance(this);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Can't setup congestion control class <" + clazzP + ">, using default.", e);
            ccObject = new BoltCongestionControl(this);
        }
        cc = (CongestionControl) ccObject;
        logger.info("Using " + cc.getClass().getName());
    }


    public abstract void received(BoltPacket packet, Destination peer);


    public BoltSocket getSocket() {
        return socket;
    }

    public void setSocket(BoltSocket socket) {
        this.socket = socket;
    }

    public CongestionControl getCongestionControl() {
        return cc;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        logger.info(toString() + " connection state CHANGED to <" + state + ">");
        this.state = state;
    }

    public void setMode(int mode) {
        this.mode = mode;
    }

    public boolean isReady() {
        return state == ready;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public boolean isShutdown() {
        return state == shutdown || state == invalid;
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

    public void setReceiveBufferSize(int bufferSize) {
        this.receiveBufferSize = bufferSize;
    }

    public int getFlowWindowSize() {
        return flowWindowSize;
    }

    public void setFlowWindowSize(int flowWindowSize) {
        this.flowWindowSize = flowWindowSize;
    }

    public BoltStatistics getStatistics() {
        return statistics;
    }

    public long getSocketID() {
        return mySocketID;
    }


    public synchronized long getInitialSequenceNumber() {
        if (initialSequenceNumber == null) {
            initialSequenceNumber = SequenceNumber.random();
        }
        return initialSequenceNumber;
    }

    public synchronized void setInitialSequenceNumber(long initialSequenceNumber) {
        this.initialSequenceNumber = initialSequenceNumber;
    }

    public DatagramPacket getDatagram() {
        return dgPacket;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append(" [");
        sb.append("socketID=").append(this.mySocketID);
        sb.append(" ]");
        return sb.toString();
    }

}
