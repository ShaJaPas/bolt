package io.lyracommunity.bolt.packet;

import io.lyracommunity.bolt.BoltSession;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;

/**
 * Protocol Connection Handshake
 * <p>
 * Additional Info: Undefined
 * <p>
 * Control Info:
 * <ol>
 * <li> 32 bits: Bolt version
 * <li> 32 bits: Socket Type (STREAM or DGRAM)
 * <li> 32 bits: initial packet sequence number
 * <li> 32 bits: maximum packet size (including UDP/IP headers)
 * <li> 32 bits: maximum flow window size
 * <li> 32 bits: connection type (regular or rendezvous)
 * <li> 32 bits: socket ID
 * <li> 32 bits: SYN cookie
 * <li> 128 bits: the IP address of the peer's UDP socket
 * </ol>
 */
public class ConnectionHandshake extends ControlPacket
{
    public static final long SOCKET_TYPE_STREAM         = 0;
    public static final long SOCKET_TYPE_DGRAM          = 1;
    public static final long CONNECTION_TYPE_REGULAR    = 1L;
    public static final long CONNECTION_TYPE_RENDEZVOUS = 0L;
    /**
     * connection type in response handshake packet
     */
    public static final long CONNECTION_SERVER_ACK      = -1L;
    private             long boltVersion                = 4;
    private             long socketType                 = SOCKET_TYPE_DGRAM;  // Stream or dgram TODO is stream needed?
    private             int  initialSeqNo               = 0;
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

    public ConnectionHandshake()
    {
        this.controlPacketType = ControlPacketType.CONNECTION_HANDSHAKE.getTypeId();
    }

    public ConnectionHandshake(byte[] controlInformation) throws IOException
    {
        this();
        decode(controlInformation);
    }

    private ConnectionHandshake(long packetSize, long boltVersion, int initialSeqNo, long connectionType, long maxFlowWndSize,
            int socketID, int destinationID, long cookie, InetAddress address)
    {
        this();
        this.packetSize = packetSize;
        this.boltVersion = boltVersion;
        this.initialSeqNo = initialSeqNo;
        this.connectionType = connectionType;
        this.maxFlowWndSize = maxFlowWndSize;
        this.socketID = socketID;
        this.destinationID = destinationID;
        this.cookie = cookie;
        this.address = address;
    }

    public static ConnectionHandshake ofClientInitial(long packetSize, int initialSeqNo, long maxFlowWndSize,
            int socketID, InetAddress address) {
        return new ConnectionHandshake(packetSize, 4, initialSeqNo, CONNECTION_TYPE_REGULAR, maxFlowWndSize, socketID,
                0, 0, address);
    }

    public static ConnectionHandshake ofClientSecond(long packetSize, int initialSeqNo, long maxFlowWndSize,
            int socketID, int destinationID, long cookie, InetAddress address) {
        return new ConnectionHandshake(packetSize, 4, initialSeqNo, CONNECTION_TYPE_REGULAR, maxFlowWndSize, socketID,
                destinationID, cookie, address);
    }

    public static ConnectionHandshake ofServerHandshakeResponse(long packetSize, int initialSeqNo, long maxFlowWndSize,
            int socketID, int destinationID, long cookie, InetAddress address) {
        return new ConnectionHandshake(packetSize, 4, initialSeqNo, CONNECTION_SERVER_ACK, maxFlowWndSize, socketID,
                destinationID, cookie, address);
    }

    /**
     * High-performance test (faster than instanceof).
     */
    public boolean isConnectionHandshake()
    {
        return true;
    }

    void decode(byte[] data) throws IOException
    {
        boltVersion = PacketUtil.decode(data, 0);
        socketType = PacketUtil.decode(data, 4);
        initialSeqNo = PacketUtil.decodeInt(data, 8);
        packetSize = PacketUtil.decode(data, 12);
        maxFlowWndSize = PacketUtil.decode(data, 16);
        connectionType = PacketUtil.decode(data, 20);
        socketID = PacketUtil.decodeInt(data, 24);
        cookie = PacketUtil.decode(data, 28);
        // TODO ipv6 check
        address = PacketUtil.decodeInetAddress(data, 32, false);
    }

    public long getBoltVersion()
    {
        return boltVersion;
    }

    public void setBoltVersion(long boltVersion)
    {
        this.boltVersion = boltVersion;
    }

    public long getSocketType()
    {
        return socketType;
    }

    public void setSocketType(long socketType)
    {
        this.socketType = socketType;
    }

    public int getInitialSeqNo()
    {
        return initialSeqNo;
    }

    public void setInitialSeqNo(int initialSeqNo)
    {
        this.initialSeqNo = initialSeqNo;
    }

    public long getPacketSize()
    {
        return packetSize;
    }

    public void setPacketSize(long packetSize)
    {
        this.packetSize = packetSize;
    }

    public long getMaxFlowWndSize()
    {
        return maxFlowWndSize;
    }

    public void setMaxFlowWndSize(long maxFlowWndSize)
    {
        this.maxFlowWndSize = maxFlowWndSize;
    }

    public long getConnectionType()
    {
        return connectionType;
    }

    public void setConnectionType(long connectionType)
    {
        this.connectionType = connectionType;
    }

    public int getSocketID()
    {
        return socketID;
    }

    public void setSocketID(int socketID)
    {
        this.socketID = socketID;
    }

    public long getCookie()
    {
        return cookie;
    }

    public void setCookie(long cookie)
    {
        this.cookie = cookie;
    }

    public InetAddress getAddress()
    {
        return address;
    }

    public void setAddress(InetAddress address)
    {
        this.address = address;
    }

    @Override
    public byte[] encodeControlInformation()
    {
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(48);
            bos.write(PacketUtil.encode(boltVersion));
            bos.write(PacketUtil.encode(socketType));
            bos.write(PacketUtil.encode(initialSeqNo));
            bos.write(PacketUtil.encode(packetSize));
            bos.write(PacketUtil.encode(maxFlowWndSize));
            bos.write(PacketUtil.encode(connectionType));
            bos.write(PacketUtil.encode(socketID));
            bos.write(PacketUtil.encode(cookie));
            bos.write(PacketUtil.encode(address));
            return bos.toByteArray();
        }
        catch (Exception e)
        {
            // can't happen
            return null;
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        ConnectionHandshake other = (ConnectionHandshake) obj;
        if (connectionType != other.connectionType)
            return false;
        if (initialSeqNo != other.initialSeqNo)
            return false;
        if (maxFlowWndSize != other.maxFlowWndSize)
            return false;
        if (packetSize != other.packetSize)
            return false;
        if (socketID != other.socketID)
            return false;
        if (socketType != other.socketType)
            return false;
        if (boltVersion != other.boltVersion)
            return false;
        if (cookie != other.cookie)
            return false;
        if (!address.equals(other.address))
            return false;
        return true;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("ConnectionHandshake [");
        sb.append("connectionType=").append(connectionType);
        BoltSession session = getSession();
        if (session != null)
        {
            sb.append(", ");
            sb.append(session.getDestination());
        }
        sb.append(", mySocketID=").append(socketID);
        sb.append(", initialSeqNo=").append(initialSeqNo);
        sb.append(", packetSize=").append(packetSize);
        sb.append(", maxFlowWndSize=").append(maxFlowWndSize);
        sb.append(", socketType=").append(socketType);
        sb.append(", destSocketID=").append(destinationID);
        if (cookie > 0)
            sb.append(", cookie=").append(cookie);
        sb.append(", address=").append(address);
        sb.append("]");
        return sb.toString();
    }

}
