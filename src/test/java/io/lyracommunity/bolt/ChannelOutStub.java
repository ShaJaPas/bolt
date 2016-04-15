package io.lyracommunity.bolt;

import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.packet.BoltPacket;
import io.lyracommunity.bolt.packet.PacketType;
import io.lyracommunity.bolt.session.SessionState;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by keen on 15/04/16.
 */
public class ChannelOutStub implements ChannelOut {

    private boolean open;

    private final InetAddress localAddress;

    private final int localPort;

    private final List<BoltPacket> sent = new ArrayList<>();

    public ChannelOutStub(final Config config, final boolean open) {
        this.localAddress = config.getLocalAddress();
        this.localPort = config.getLocalPort();
        this.open = open;
    }

    @Override
    public void doSend(BoltPacket packet, SessionState sessionState) throws IOException {
        if (isOpen()) sent.add(packet);
        else throw new IOException("Endpoint is closed");
    }

    public long sendCountOfType(final PacketType packetType) {
        return sent.stream().filter(p -> packetType == p.getPacketType()).count();
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public InetAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public int getLocalPort() {
        return localPort;
    }

    public void setOpen(boolean open) {
        this.open = open;
    }

}
