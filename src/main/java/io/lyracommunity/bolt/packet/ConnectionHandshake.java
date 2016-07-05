package io.lyracommunity.bolt.packet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Objects;

/**
 * Protocol Connection Handshake
 * <p>
 * Additional Info: Undefined
 * <p>
 * Control Info:
 * <ol>
 * <li> 32 bits: Bolt version
 * <li> 32 bits: initial packet sequence number
 * <li> 32 bits: maximum packet size (including UDP/IP headers)
 * <li> 32 bits: maximum flow window size
 * <li> 32 bits: socket ID
 * <li> 32 bits: SYN cookie
 * <li> 128 bits: the IP address of the peer's UDP socket
 * </ol>
 */
public class ConnectionHandshake extends ControlPacket {


    /**
     * Response type in response handshake packet.
     */
    public static final long SERVER_FIRST_ACK_HANDSHAKE = -1L;

    /**
     * Sent by server on completion of handshake process.
     */
    public static final long SERVER_FINISHED_HANDSHAKE = -2L;

    private static final long CLIENT_HANDSHAKE = 1L;

    private static final long BOLT_VERSION = 1;


    private long boltVersion;
    private int initialSeqNo = 0;
    private long packetSize;
    private long maxFlowWndSize;
    private long handshakeType = CLIENT_HANDSHAKE;  // Regular or rendezvous mode

    /**
     * Tell peer what the socket ID on this side is.
     */
    private int sessionID;

    private long cookie = 0;

    // Address of the UDP socket
    private InetAddress address;

    private ConnectionHandshake() {
        super(PacketType.HANDSHAKE);
    }

    ConnectionHandshake(byte[] controlInformation) throws IOException {
        super(PacketType.HANDSHAKE, controlInformation);
        decode(controlInformation);
    }

    ConnectionHandshake(long packetSize, int initialSeqNo, long boltVersion, long handshakeType, long maxFlowWndSize, int sessionID,
                        int destinationID, long cookie, InetAddress address) {
        this();
        Objects.requireNonNull(address);
        this.packetSize = packetSize;
        this.initialSeqNo = initialSeqNo;
        this.boltVersion = boltVersion;
        this.handshakeType = handshakeType;
        this.maxFlowWndSize = maxFlowWndSize;
        this.sessionID = sessionID;
        this.destinationID = destinationID;
        this.cookie = cookie;
        this.address = address;
    }

    /**
     * Build first stage of three-way handshake.
     * This is the first client to server handshake request.
     */
    public static ConnectionHandshake ofClientInitial(long packetSize, int initialSeqNo, long maxFlowWndSize,
                                                      int sessionID, InetAddress address) {
        return new ConnectionHandshake(packetSize, initialSeqNo, BOLT_VERSION, CLIENT_HANDSHAKE, maxFlowWndSize, sessionID, 0, 0, address);
    }

    /**
     * Build the third and final stage of the three-way handshake.
     * This is the client's acknowledgement of the previous server acknowledgement.
     */
    public static ConnectionHandshake ofClientSecond(long packetSize, int initialSeqNo, long maxFlowWndSize,
                                                     int sessionID, int destinationID, long cookie, InetAddress address) {
        return new ConnectionHandshake(packetSize, initialSeqNo, BOLT_VERSION, CLIENT_HANDSHAKE, maxFlowWndSize, sessionID,
                destinationID, cookie, address);
    }

    /**
     * Build the second stage of the three-way handshake.
     * This is the server response to the client handshake request.
     */
    public static ConnectionHandshake ofServerFirstCookieShareResponse(long packetSize, int initialSeqNo, long maxFlowWndSize,
                                                                       int sessionID, int destinationID, long cookie, InetAddress address) {
        return new ConnectionHandshake(packetSize, initialSeqNo, BOLT_VERSION, SERVER_FIRST_ACK_HANDSHAKE, maxFlowWndSize, sessionID,
                destinationID, cookie, address);
    }

    /**
     * Build the second stage of the three-way handshake.
     * This is the server response to the client handshake request.
     */
    public static ConnectionHandshake ofServerFinalResponse(long packetSize, int initialSeqNo, long maxFlowWndSize,
                                                            int sessionID, int destinationID, long cookie, InetAddress address) {
        return new ConnectionHandshake(packetSize, initialSeqNo, BOLT_VERSION, SERVER_FINISHED_HANDSHAKE, maxFlowWndSize, sessionID,
                destinationID, cookie, address);
    }

    public long getBoltVersion() {
        return boltVersion;
    }

    public int getInitialSeqNo() {
        return initialSeqNo;
    }

    public long getPacketSize() {
        return packetSize;
    }

    public long getMaxFlowWndSize() {
        return maxFlowWndSize;
    }

    public long getHandshakeType() {
        return handshakeType;
    }

    public int getSessionID() {
        return sessionID;
    }

    public long getCookie() {
        return cookie;
    }

    public InetAddress getAddress() {
        return address;
    }

    protected void decode(final byte[] data) throws IOException {
        boltVersion = PacketUtil.decode(data, 0);
        initialSeqNo = PacketUtil.decodeInt(data, 4);
        packetSize = PacketUtil.decode(data, 8);
        maxFlowWndSize = PacketUtil.decode(data, 12);
        handshakeType = PacketUtil.decode(data, 16);
        sessionID = PacketUtil.decodeInt(data, 20);
        cookie = PacketUtil.decode(data, 24);
        // TODO ipv6 check
        address = PacketUtil.decodeInetAddress(data, 28, false);
    }

    @Override
    public byte[] encodeControlInformation() {
        try {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream(44);
            bos.write(PacketUtil.encode(boltVersion));
            bos.write(PacketUtil.encode(initialSeqNo));
            bos.write(PacketUtil.encode(packetSize));
            bos.write(PacketUtil.encode(maxFlowWndSize));
            bos.write(PacketUtil.encode(handshakeType));
            bos.write(PacketUtil.encode(sessionID));
            bos.write(PacketUtil.encode(cookie));
            bos.write(PacketUtil.encode(address));
            return bos.toByteArray();
        }
        catch (Exception e) {
            // can't happen
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        final ConnectionHandshake that = (ConnectionHandshake) o;
        return boltVersion == that.boltVersion &&
                initialSeqNo == that.initialSeqNo &&
                packetSize == that.packetSize &&
                maxFlowWndSize == that.maxFlowWndSize &&
                handshakeType == that.handshakeType &&
                sessionID == that.sessionID &&
                cookie == that.cookie &&
                Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(super.hashCode(), boltVersion, initialSeqNo, packetSize, maxFlowWndSize, handshakeType, sessionID, cookie, address);
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ConnectionHandshake [");
        sb.append("handshakeType=").append(handshakeType);
        sb.append(", sessionID=").append(sessionID);
        sb.append(", initialSeqNo=").append(initialSeqNo);
        sb.append(", packetSize=").append(packetSize);
        sb.append(", maxFlowWndSize=").append(maxFlowWndSize);
        sb.append(", destSocketID=").append(destinationID);
        if (cookie > 0) {
            sb.append(", cookie=").append(cookie);
        }
        sb.append(", address=").append(address);
        sb.append("]");
        return sb.toString();
    }

}
