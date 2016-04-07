package io.lyracommunity.bolt.session;

import io.lyracommunity.bolt.packet.Destination;
import io.lyracommunity.bolt.util.SeqNum;

import java.net.DatagramPacket;

/**
 * Created by keen on 07/04/16.
 */
public class SessionState {


    /**
     * Remote Bolt entity (address and socket ID).
     */
    protected final Destination destination;
    /**
     * The size of the receive buffer.
     */
    private final int receiveBufferSize = 64 * 32768;
    /**
     * Session cookie created during handshake.
     */
    protected long sessionCookie = 0;
    /**
     * Flow window size (how many data packets are in-flight at a single time).
     */
    protected int flowWindowSize = 1024 * 10;
    /**
     * Initial packet sequence number.
     */
    protected Integer initialSequenceNumber = null;
    /**
     * Cache dgPacket (peer stays the same always).
     */
    private DatagramPacket dgPacket;
    private volatile SessionStatus status = SessionStatus.START;


    public SessionState(Destination destination) {
        this.destination = destination;
        this.dgPacket = new DatagramPacket(new byte[0], 0, destination.getAddress(), destination.getPort());
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

    public void setDestinationSocketID(final int destSocketID) {
        destination.setSocketID(destSocketID);
    }

    public boolean isReady() {
        return status == SessionStatus.READY;
    }

    public boolean isShutdown() {
        return status == SessionStatus.SHUTDOWN || status == SessionStatus.INVALID;
    }

    public SessionStatus getStatus() {
        return status;
    }

    public void setStatus(final SessionStatus status) {
        this.status = status;
    }

    public long getSessionCookie() {
        return sessionCookie;
    }

    public void setSessionCookie(long sessionCookie) {
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

}
