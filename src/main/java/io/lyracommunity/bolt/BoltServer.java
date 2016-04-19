package io.lyracommunity.bolt;

import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.api.Server;
import io.lyracommunity.bolt.codec.CodecRepository;
import io.lyracommunity.bolt.event.ReceiveObject;
import io.lyracommunity.bolt.packet.DataPacket;
import io.lyracommunity.bolt.session.Session;
import io.lyracommunity.bolt.session.SessionController;
import io.lyracommunity.bolt.statistic.BoltStatistics;
import io.lyracommunity.bolt.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Created by omahoc9 on 3/3/16.
 */
public class BoltServer implements Server
{

    private static final Logger LOG = LoggerFactory.getLogger(BoltServer.class);

    private final CodecRepository codecs;

    private final Config config;

    private volatile Endpoint serverEndpoint;

    private final SessionController serverSessions;


    public BoltServer(final Config config) {
        this(CodecRepository.basic(), config);
    }

    private BoltServer(final CodecRepository codecs, final Config config) {
        this.codecs = codecs;
        this.config = config;
        this.serverSessions = new SessionController(config, true);
    }

    @Override
    public Observable<?> bind() {
        return Observable.create(subscriber -> {
            Subscription endpointSub = null;
            try {
                Thread.currentThread().setName("Bolt-Poller-Server" + Util.THREAD_INDEX.incrementAndGet());
                this.serverEndpoint = new Endpoint("ServerEndpoint", config, serverSessions);
                endpointSub = this.serverEndpoint.start().subscribe(subscriber);  // Pass subscriber to tie observable life-cycles together.

                while (!subscriber.isUnsubscribed()) {
                    pollReceivedData(subscriber);
                }
            }
            catch (InterruptedException ex) {
                // Do nothing.
            }
            catch (Exception ex) {
                subscriber.onError(ex);
            }
            if (endpointSub != null) {
                LOG.info("Enforcing endpoint stop by server");
                serverEndpoint.stop(subscriber);
                endpointSub.unsubscribe();
            }
            subscriber.onCompleted();
        });
    }

    private void pollReceivedData(final Subscriber<? super Object> subscriber) throws InterruptedException {
//        serverSessions.awaitMoreWork(50, TimeUnit.MILLISECONDS);
        for (Session session : serverSessions.getSessions()) {
            final DataPacket packet = session.pollReceiveBuffer(1, TimeUnit.MILLISECONDS);

            if (packet != null) {
                final Object decoded = codecs.decode(packet, session.getAssembleBuffer());
                if (decoded != null) {
                    subscriber.onNext(new ReceiveObject<>(session.getSocketID(), decoded));
                }
            }
        }

    }

    public int getPort() {
        return (serverEndpoint != null) ? serverEndpoint.getLocalPort() : config.getLocalPort();
    }

    public void sendToAll(final Object obj) throws IOException {
        final List<Integer> ids = serverSessions.getSessions().stream()
                .map(Session::getSocketID)
                .collect(Collectors.toList());
        send(obj, ids);
    }

    @Override
    public void send(final Object obj, final int destID) throws IOException {
        send(obj, Collections.singletonList(destID));
    }

    public void send(final Object obj, final List<Integer> destIDs) throws IOException {
        Collection<DataPacket> data = null;
        for (final Integer destID : destIDs) {
            final Session session = Optional.ofNullable(serverSessions).map(e -> e.getSession(destID)).orElse(null);
            if (session != null) {
                if (data == null) data = codecs.encode(obj, session.getAssembleBuffer());
                for (final DataPacket dp : data) {
                    session.doWrite(dp);
                }
            }
        }
    }

    public List<BoltStatistics> getStatistics() {
        return serverSessions.getSessions().stream().map(Session::getStatistics).collect(Collectors.toList());
    }

    public Config config() {
        return config;
    }

    public CodecRepository codecs() {
        return codecs;
    }

}
