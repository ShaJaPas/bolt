package io.lyracommunity.bolt;

import io.lyracommunity.bolt.api.BoltEvent;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.packet.BoltPacket;
import io.lyracommunity.bolt.packet.Destination;
import io.lyracommunity.bolt.packet.PacketFactory;
import io.lyracommunity.bolt.receiver.ReceiverThread;
import io.lyracommunity.bolt.sender.SenderThread;
import io.lyracommunity.bolt.session.Session;
import io.lyracommunity.bolt.session.SessionController;
import io.lyracommunity.bolt.session.SessionState;
import io.lyracommunity.bolt.util.NetworkQoSSimulationPipeline;
import io.lyracommunity.bolt.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.net.*;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;

/**
 * The Endpoint takes care of sending and receiving UDP network packets,
 * dispatching them to the {@link SessionController}.
 *
 * @author Cian.
 */
class Endpoint implements ChannelOut {

    private static final Logger LOG = LoggerFactory.getLogger(Endpoint.class);

    private final DatagramPacket dp = new DatagramPacket(new byte[Config.DEFAULT_DATAGRAM_SIZE], Config.DEFAULT_DATAGRAM_SIZE);
    private final int            port;
    private final DatagramSocket dgSocket;
    private final Config         config;
    private final String         name;

    private final SessionController sessionController;

    private final ReceiverThread receiverThread;

    private final SenderThread senderThread;

    /**
     * Bind to the given address and port.
     *
     * @param name   name of the endpoint, eg "ClientEndpoint" or "ServerEndpoint".
     * @param config config containing the address information to bind.
     * @throws SocketException if for example if the port is already bound to.
     */
    Endpoint(final String name, final Config config, final SessionController sessionController) throws SocketException {
        this.config = config;
        this.name = name;
        this.sessionController = sessionController;
        this.dgSocket = new DatagramSocket(config.getLocalPort(), config.getLocalAddress());
        // If the port is zero, the system will pick an ephemeral port.
        this.port = (config.getLocalPort() > 0) ? config.getLocalPort() : dgSocket.getLocalPort();
        this.receiverThread = new ReceiverThread(config, sessionController);
        this.senderThread = new SenderThread(sessionController);
        configureSocket();
    }

    private void configureSocket() throws SocketException {
        // set a time out to avoid blocking in doReceive()
        dgSocket.setSoTimeout(50_000);
        // buffer size
        dgSocket.setReceiveBufferSize(128 * 1024);
        dgSocket.setReuseAddress(false);
    }

    /**
     * Start the endpoint.
     *
     * @return the stream of events.
     */
    public Observable<BoltEvent> start() {
        return Observable.merge(
                receiverThread.start().subscribeOn(Schedulers.io()),
                senderThread.start().subscribeOn(Schedulers.io()),
                Observable.<BoltEvent>create(this::doReceive).subscribeOn(Schedulers.io()));
    }

    void stop(final Subscriber<? super BoltEvent> subscriber) {
        LOG.info("Stopping {}", name);
        sessionController.stop(subscriber, name + " is closing.");
        dgSocket.close();
    }

    @Override
    public int getLocalPort() {
        return this.dgSocket.getLocalPort();
    }

    DatagramSocket getSocket() {
        return dgSocket;
    }

    /**
     * Single receive, run in the receiverThread, see {@link #start()}.
     * <ul>
     * <li>Receives UDP packets from the network.
     * <li>Converts them to Bolt packets.
     * <li>dispatches the Bolt packets according to their destination ID.
     * </ul>
     */
    private void doReceive(final Subscriber<? super BoltEvent> subscriber) {
        Thread.currentThread().setName("Bolt-" + name + "-" + Util.THREAD_INDEX.incrementAndGet());
        LOG.info("{} started.", name);

        final NetworkQoSSimulationPipeline qosSimulationPipeline = new NetworkQoSSimulationPipeline(config,
                (peer, pkt) -> sessionController.processPacket(subscriber, peer, pkt, this),
                (peer, pkt) -> markPacketAsDropped(pkt));
        while (!subscriber.isUnsubscribed()) {
            try {
                // Will block until a packet is received or timeout has expired.
                dgSocket.receive(dp);

                final Destination peer = new Destination(dp.getAddress(), dp.getPort());
                final int l = dp.getLength();
                final BoltPacket packet = PacketFactory.createPacket(dp.getData(), l);

                if (LOG.isDebugEnabled()) LOG.debug("{} received packet {}", name, packet.getPacketSeqNumber());

                qosSimulationPipeline.offer(peer, packet);

            }
            catch (AsynchronousCloseException ex) {
                LOG.info("{} interrupted.", name);
            }
            catch (SocketTimeoutException ex) {
                LOG.debug("{} socket timeout", name);
            }
            catch (SocketException ex) {
                LOG.warn("{} SocketException: {}", name, ex.getMessage());
                if (dgSocket.isClosed()) subscriber.onError(ex);
            }
            catch (ClosedChannelException ex) {
                LOG.warn("{} Channel was closed but receive was attempted", name);
            }
            catch (Exception ex) {
                LOG.error("{} Unexpected endpoint error", name, ex);
            }
        }
        LOG.info("{} completed naturally.", name);
        qosSimulationPipeline.close();
        stop(subscriber);
    }

    private void markPacketAsDropped(final BoltPacket packet) {
        final Session session = sessionController.getSession(packet.getDestinationSessionID());
        if (session != null) session.getStatistics().incNumberOfArtificialDrops();
    }

    @Override
    public InetAddress getLocalAddress() {
        return this.dgSocket.getLocalAddress();
    }

    @Override
    public boolean isOpen() {
        return !dgSocket.isClosed();
    }

    @Override
    public void doSend(final BoltPacket packet, final SessionState sessionState) throws IOException {
        final byte[] data = packet.getEncoded();
        final DatagramPacket dgp = sessionState.getDatagram();
        dgp.setData(data);
        dgSocket.send(dgp);
    }

    public String toString() {
        return name + " port=" + port;
    }

    void sendRaw(DatagramPacket p) throws IOException {
        dgSocket.send(p);
    }

    public Config getConfig() {
        return config;
    }

}
