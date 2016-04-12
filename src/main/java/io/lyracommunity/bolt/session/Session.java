package io.lyracommunity.bolt.session;

import io.lyracommunity.bolt.BoltCongestionControl;
import io.lyracommunity.bolt.ChannelOut;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.CongestionControl;
import io.lyracommunity.bolt.packet.BoltPacket;
import io.lyracommunity.bolt.packet.ConnectionHandshake;
import io.lyracommunity.bolt.packet.DataPacket;
import io.lyracommunity.bolt.packet.Destination;
import io.lyracommunity.bolt.packet.Shutdown;
import io.lyracommunity.bolt.receiver.Receiver;
import io.lyracommunity.bolt.sender.Sender;
import io.lyracommunity.bolt.statistic.BoltStatistics;
import io.lyracommunity.bolt.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


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
public abstract class Session
{

    private static final Logger LOG = LoggerFactory.getLogger(Session.class);

    final SessionState      state;
    final CongestionControl cc;
    final ChannelOut        endPoint;

    /** Statistics for the session. */
    private final BoltStatistics statistics;

    /** Buffer size (i.e. datagram size). This is negotiated during connection setup. */
    private int datagramSize = Config.DEFAULT_DATAGRAM_SIZE;

    // Processing received data
    final Receiver receiver;
    final Sender   sender;


    Session(final Config config, final ChannelOut endpoint, final Destination destination, final String description) {
        this.endPoint = endpoint;
        this.statistics = new BoltStatistics(description, datagramSize);
        this.state = new SessionState(destination);
        this.cc = new BoltCongestionControl(state, statistics);

        this.sender = new Sender(config, state, endpoint, cc, statistics);
        this.receiver = new Receiver(config, state, endpoint, sender, statistics);
    }

    public abstract void received(BoltPacket packet, Subscriber subscriber);

    public abstract boolean receiveHandshake(Subscriber<? super Object> subscriber, ConnectionHandshake handshake, Destination peer);


    public Observable<?> start() throws IllegalStateException {
        if (state.isActive()) throw new IllegalStateException();

        final String threadSuffix = ((this instanceof ServerSession) ? "Server" : "Client") + "Session-"
                + getSocketID() + "-" + Util.THREAD_INDEX.incrementAndGet();
        return Observable.merge(
                receiver.start(threadSuffix).subscribeOn(Schedulers.io()),
                sender.doStart(threadSuffix).subscribeOn(Schedulers.io()))
                .doOnSubscribe(() -> state.setActive(true));
    }

    public void cleanup() {
        try {
            close();
            if (endPoint.isOpen()) {
                endPoint.doSend(new Shutdown(state.getDestinationSocketID()), state);
            }
        }
        catch (IOException ex) {
            LOG.warn("Could not cleanup Session cleanly: {}", ex.getMessage());
        }
    }

    public void doWrite(final DataPacket dataPacket) throws IOException {
        try {
            sender.sendPacket(dataPacket, 10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }
        if (dataPacket.getDataLength() > 0) state.setActive(true);
    }

    protected void doWriteBlocking(final DataPacket dataPacket) throws IOException, InterruptedException {
        doWrite(dataPacket);
        flush();
    }

    /**
     * Will block until the outstanding packets have really been sent out
     * and acknowledged.
     */
    public void flush() throws InterruptedException, IllegalStateException {
        if (!state.isActive()) return;
        final int seqNo = sender.getCurrentSequenceNumber();
        final int relSeqNo = sender.getCurrentReliabilitySequenceNumber();
        if (seqNo < 0) throw new IllegalStateException();
        while (state.isActive() && !sender.isSentOut(seqNo)) {
            Thread.sleep(5);
        }
        if (seqNo > -1) {
            // Wait until data has been sent out and acknowledged.
            while (state.isActive() && !sender.haveAcknowledgementFor(relSeqNo)) {
                sender.waitForAck(relSeqNo);
            }
        }
    }

    public DataPacket pollReceiveBuffer(final int timeout, final TimeUnit unit) throws InterruptedException {
        return receiver.pollReceiveBuffer(timeout, unit);
    }

    /**
     * Close the connection.
     *
     * @throws IOException
     */
    public void close() throws IOException {
        setStatus(SessionStatus.SHUTDOWN);
        state.setActive(false);
    }

    public boolean isStarted() {
        return state.isActive();
    }


    public Sender getSender() {
        return sender;
    }

    SessionStatus getStatus() {
        return state.getStatus();
    }

    void setStatus(final SessionStatus status) {
        LOG.info("{} connection status CHANGED to [{}]", this, status);
        state.setStatus(status);
    }

    public int getSocketID() {
        return state.getSocketID();
    }

    int getDatagramSize() {
        return datagramSize;
    }

    void setDatagramSize(int datagramSize) {
        this.datagramSize = datagramSize;
    }

    public BoltStatistics getStatistics() {
        return statistics;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" + state + '}';
    }

}
