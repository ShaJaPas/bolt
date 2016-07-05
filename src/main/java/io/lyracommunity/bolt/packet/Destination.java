package io.lyracommunity.bolt.packet;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Objects;

public class Destination {

    private final int port;

    private final InetAddress address;

    private final InetSocketAddress socketAddress;

    /** Bolt socket ID of the peer */
    private volatile int sessionID;

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

    public int getSessionID() {
        return sessionID;
    }

    public void setSessionID(final int sessionID) {
        this.sessionID = sessionID;
    }

    public String toString() {
        return ("Destination [" + address.getHostName() + " port=" + port + " sessionID=" + sessionID) + "]";
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
                sessionID == that.sessionID &&
                Objects.equals(address, that.address);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(port, address, sessionID);
    }


}
