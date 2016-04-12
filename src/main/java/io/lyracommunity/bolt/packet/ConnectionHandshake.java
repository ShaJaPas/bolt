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


    private static final long CONNECTION_TYPE_REGULAR = 1L;

    /** Connection type in response handshake packet. */
    public static final long CONNECTION_SERVER_ACK = -1L;

    private static final long BOLT_VERSION = 1;


    private long boltVersion;
    private int initialSeqNo = 0;
    private long packetSize;
    private long maxFlowWndSize;
    private long connectionType = CONNECTION_TYPE_REGULAR;  // Regular or rendezvous mode

    /**
     * Tell peer what the socket ID on this side is.
     */
    private int socketID;

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

    ConnectionHandshake(long packetSize, int initialSeqNo, long boltVersion, long connectionType, long maxFlowWndSize, int socketID,
            int destinationID, long cookie, InetAddress address) {
        this();
        Objects.requireNonNull(address);
        this.packetSize = packetSize;
        this.initialSeqNo = initialSeqNo;
        this.boltVersion = boltVersion;
        this.connectionType = connectionType;
        this.maxFlowWndSize = maxFlowWndSize;
        this.socketID = socketID;
        this.destinationID = destinationID;
        this.cookie = cookie;
        this.address = address;
    }

    /**
     * Build first stage of three-way handshake.
     * This is the first client to server handshake request.
     */
    public static ConnectionHandshake ofClientInitial(long packetSize, int initialSeqNo, long maxFlowWndSize,
                                                      int socketID, InetAddress address) {
        return new ConnectionHandshake(packetSize, initialSeqNo, BOLT_VERSION, CONNECTION_TYPE_REGULAR, maxFlowWndSize, socketID, 0, 0, address);
    }

    /**
     * Build the third and final stage of the three-way handshake.
     * This is the client's acknowledgement of the previous server acknowledgement.
     */
    public static ConnectionHandshake ofClientSecond(long packetSize, int initialSeqNo, long maxFlowWndSize,
                                                     int socketID, int destinationID, long cookie, InetAddress address) {
        return new ConnectionHandshake(packetSize, initialSeqNo, BOLT_VERSION, CONNECTION_TYPE_REGULAR, maxFlowWndSize, socketID,
                destinationID, cookie, address);
    }

    /**
     * Build the second stage of the three-way handshake.
     * This is the server response to the client handshake request.
     */
    public static ConnectionHandshake ofServerHandshakeResponse(long packetSize, int initialSeqNo, long maxFlowWndSize,
                                                                int socketID, int destinationID, long cookie, InetAddress address) {
        return new ConnectionHandshake(packetSize, initialSeqNo, BOLT_VERSION, CONNECTION_SERVER_ACK, maxFlowWndSize, socketID,
                destinationID, cookie, address);
    }

    protected void decode(final byte[] data) throws IOException {
        boltVersion = PacketUtil.decode(data, 0);
        initialSeqNo = PacketUtil.decodeInt(data, 4);
        packetSize = PacketUtil.decode(data, 8);
        maxFlowWndSize = PacketUtil.decode(data, 12);
        connectionType = PacketUtil.decode(data, 16);
        socketID = PacketUtil.decodeInt(data, 20);
        cookie = PacketUtil.decode(data, 24);
        // TODO ipv6 check
        address = PacketUtil.decodeInetAddress(data, 28, false);
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

    public long getConnectionType() {
        return connectionType;
    }

    public int getSocketID() {
        return socketID;
    }

    public long getCookie() {
        return cookie;
    }

    public InetAddress getAddress() {
        return address;
    }

    @Override
    public byte[] encodeControlInformation() {
        try {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream(44);
            bos.write(PacketUtil.encode(boltVersion));
            bos.write(PacketUtil.encode(initialSeqNo));
            bos.write(PacketUtil.encode(packetSize));
            bos.write(PacketUtil.encode(maxFlowWndSize));
            bos.write(PacketUtil.encode(connectionType));
            bos.write(PacketUtil.encode(socketID));
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
    public boolean equals(Object o)
    {
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
                connectionType == that.connectionType &&
                socketID == that.socketID &&
                cookie == that.cookie &&
                Objects.equals(address, that.address);
    }

    @Override
    public int hashCode()
    {
        return Objects
                .hash(super.hashCode(), boltVersion, initialSeqNo, packetSize, maxFlowWndSize, connectionType, socketID, cookie, address);
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ConnectionHandshake [");
        sb.append("connectionType=").append(connectionType);
        sb.append(", mySocketID=").append(socketID);
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
