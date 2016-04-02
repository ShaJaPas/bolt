package io.lyracommunity.bolt.packet;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;

public class Destination {

    private final int port;

    private final InetAddress address;

    private final InetSocketAddress socketAddress;

    /** Bolt socket ID of the peer */
    private int socketID;

    public Destination(InetAddress address, int port) {
        this.address = address;
        this.port = port;
        this.socketAddress = new InetSocketAddress(address, port);
    }

    public InetAddress getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public int getSocketID() {
        return socketID;
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    public void setSocketID(int socketID) {
        this.socketID = socketID;
    }

    public String toString() {
        return ("Destination [" + address.getHostName() + " port=" + port + " socketID=" + socketID) + "]";
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final Destination that = (Destination) o;
        return port == that.port &&
                socketID == that.socketID &&
                Objects.equals(address, that.address);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(port, address, socketID);
    }


}
