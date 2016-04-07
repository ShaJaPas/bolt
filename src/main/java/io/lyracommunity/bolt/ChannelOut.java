package io.lyracommunity.bolt;

import io.lyracommunity.bolt.packet.BoltPacket;
import io.lyracommunity.bolt.session.SessionState;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Created by omahoc9 on 4/7/16.
 */
public interface ChannelOut
{

    /**
     * Send a packet to the session's destination.
     *
     * @param packet       the packet to send.
     * @param sessionState the state of the session.
     * @throws IOException if the packet could not be sent or the channel was not {@link ChannelOut#isOpen() open}.
     */
    void doSend(final BoltPacket packet, final SessionState sessionState) throws IOException;

    /**
     * @return true if the channel is open, otherwise false.
     */
    boolean isOpen();

    /**
     * @return the local address to which the socket is bound.
     */
    InetAddress getLocalAddress();

}
