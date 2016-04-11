package io.lyracommunity.bolt;

import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.api.Server;
import io.lyracommunity.bolt.codec.CodecRepository;
import io.lyracommunity.bolt.codec.MessageAssembleBuffer;
import io.lyracommunity.bolt.event.ReceiveObject;
import io.lyracommunity.bolt.packet.DataPacket;
import io.lyracommunity.bolt.session.Session;
import io.lyracommunity.bolt.statistic.BoltStatistics;
import io.lyracommunity.bolt.util.Util;
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

    private final CodecRepository codecs;

    private final Config config;

    private volatile BoltEndPoint serverEndpoint;


    public BoltServer(final Config config) {
        this(CodecRepository.basic(new MessageAssembleBuffer()), config);
    }

    private BoltServer(final CodecRepository codecs, final Config config) {
        this.codecs = codecs;
        this.config = config;
    }

    @Override
    public Observable<?> bind() {
        return Observable.create(subscriber -> {
            Subscription endpointSub = null;
            try {
                Thread.currentThread().setName("Bolt-Poller-Server" + Util.THREAD_INDEX.incrementAndGet());
                this.serverEndpoint = new BoltEndPoint(config);
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
                serverEndpoint.stop(subscriber);
                endpointSub.unsubscribe();
            }
            subscriber.onCompleted();
        });
    }

    private void pollReceivedData(final Subscriber<? super Object> subscriber) throws InterruptedException {
        for (Session session : serverEndpoint.getSessions()) {
            // TODO never breaks.
            final DataPacket packet = session.pollReceiveBuffer(1, TimeUnit.MILLISECONDS);

            if (packet != null) {
                final Object decoded = codecs.decode(packet);
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
        final List<Integer> ids = serverEndpoint.getSessions().stream()
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
            final Session session = Optional.ofNullable(serverEndpoint).map(e -> e.getSession(destID)).orElse(null);
            if (session != null) {
                if (data == null) data = codecs.encode(obj);
                for (final DataPacket dp : data) {
                    session.doWrite(dp);
                }
            }
        }
    }

    public List<BoltStatistics> getStatistics() {
        return serverEndpoint.getSessions().stream().map(Session::getStatistics).collect(Collectors.toList());
    }

    public Config config() {
        return config;
    }

    public CodecRepository codecs() {
        return codecs;
    }

}
