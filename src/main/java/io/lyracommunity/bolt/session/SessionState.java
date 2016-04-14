package io.lyracommunity.bolt.session;

import io.lyracommunity.bolt.packet.Destination;
import io.lyracommunity.bolt.util.SeqNum;

import java.net.DatagramPacket;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by keen on 07/04/16.
 */
public class SessionState {


    private final static AtomicInteger NEXT_SOCKET_ID = new AtomicInteger(20 + new Random().nextInt(5000));

    /**
     * Remote Bolt entity (address and socket ID).
     */
    protected final Destination destination;
    /**
     * The size of the receive buffer.
     */
    private final int receiveBufferSize = 64 * 32768;
    /**
     * The socket ID of this session.
     */
    private final int mySocketID;
    /**
     * Session cookie created during handshake.
     */
    private long sessionCookie = 0;
    /**
     * Flow window size (how many data packets are in-flight at a single time).
     */
    private int flowWindowSize = 1024 * 10;

    // TODO review whether statstics belongs to this class or outside
    /**
     * Initial packet sequence number.
     */
    private Integer initialSequenceNumber = null;
    /**
     * Cache dgPacket (peer stays the same always).
     */
    private DatagramPacket dgPacket;
    /**
     * Whether the session is started and active.
     */
    private volatile boolean active;

    private volatile SessionStatus status = SessionStatus.START;


    public SessionState(final Destination destination) {
        this.destination = destination;
        this.dgPacket = new DatagramPacket(new byte[0], 0, destination.getAddress(), destination.getPort());
        this.mySocketID = NEXT_SOCKET_ID.incrementAndGet();
    }

    int getSocketID() {
        return mySocketID;
    }

    public int getFlowWindowSize() {
        return flowWindowSize;
    }

    public Destination getDestination() {
        return destination;
    }

    public int getDestinationSocketID() {
        return destination.getSocketID();
    }

    void setDestinationSocketID(final int destSocketID) {
        destination.setSocketID(destSocketID);
    }

    public boolean isReady() {
        return status == SessionStatus.READY;
    }

    public boolean isShutdown() {
        return status == SessionStatus.SHUTDOWN || status == SessionStatus.INVALID;
    }

    SessionStatus getStatus() {
        return status;
    }

    public void setStatus(final SessionStatus status) {
        this.status = status;
    }

    long getSessionCookie() {
        return sessionCookie;
    }

    void setSessionCookie(long sessionCookie) {
        this.sessionCookie = sessionCookie;
    }

    public synchronized int getInitialSequenceNumber() {
        if (initialSequenceNumber == null) {
            initialSequenceNumber = SeqNum.randomPacketSeqNum();
        }
        return initialSequenceNumber;
    }

    synchronized void setInitialSequenceNumber(int initialSequenceNumber) {
        this.initialSequenceNumber = initialSequenceNumber;
    }

    public DatagramPacket getDatagram() {
        return dgPacket;
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(final boolean active) {
        this.active = active;
    }

    @Override
    public String toString() {
        return "SessionState{" + "mySocketID=" + mySocketID + '}';
    }

}
