package io.lyracommunity.bolt;

import io.lyracommunity.bolt.codec.CodecRepository;
import io.lyracommunity.bolt.codec.MessageAssembleBuffer;
import io.lyracommunity.bolt.event.ReceiveObject;
import io.lyracommunity.bolt.packet.DataPacket;
import io.lyracommunity.bolt.session.BoltSession;
import io.lyracommunity.bolt.statistic.BoltStatistics;
import io.lyracommunity.bolt.util.Util;
import rx.Observable;
import rx.Subscriber;

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
public class BoltServer implements Server {

    private final CodecRepository codecs;

    private final Config config;

    private volatile BoltEndPoint serverEndpoint;

    private volatile int count;

    public BoltServer(final Config config) {
        this(CodecRepository.basic(new MessageAssembleBuffer()), config);
    }

    public BoltServer(final CodecRepository codecs, final Config config) {
        this.codecs = codecs;
        this.config = config;
    }

    @Override
    public Observable<?> bind() {
        return Observable.create(subscriber -> {
            try {
                Thread.currentThread().setName("Bolt-Poller-Server" + Util.THREAD_INDEX.incrementAndGet());
                this.serverEndpoint = new BoltEndPoint(config);
                this.serverEndpoint.start().subscribe(subscriber);

                while (!subscriber.isUnsubscribed()) {
//                    try {
                        pollReceivedData(subscriber);
//                        Thread.sleep(0, 1000);
//                    }
//                    catch (InterruptedException e) {
//                        // Do nothing.
//                    }
                }
            }
            catch (Exception ex) {
                subscriber.onError(ex);
            }
            subscriber.onCompleted();
            shutdown();
        });
//                .share();
    }

    private void pollReceivedData(final Subscriber<? super Object> subscriber) {
        for (BoltSession session : serverEndpoint.getSessions()) {
            if (session.getSocket() != null) {
                try {
                    final DataPacket packet = session.getSocket().getReceiveBuffer().poll(10, TimeUnit.MILLISECONDS);

                    if (packet != null) {
                        final Object decoded = codecs.decode(packet);
                        if (decoded != null) {
                            subscriber.onNext(new ReceiveObject<>(session.getSocketID(), decoded));
                        }
                    }
                }
                catch (InterruptedException e) {
                    // Do nothing
                }

            }
        }
    }

    public int getPort() {
        return (serverEndpoint != null) ? serverEndpoint.getLocalPort() : config.getLocalPort();
    }

    public void sendToAll(final Object obj) throws IOException {
        final List<Long> ids = serverEndpoint.getSessions().stream()
                .map(BoltSession::getSocketID)
                .collect(Collectors.toList());
        send(obj, ids);
    }

    @Override
    public void send(final Object obj, final long destID) throws IOException {
        send(obj, Collections.singletonList(destID));
    }

    public void send(final Object obj, final List<Long> destIDs) throws IOException {
        Collection<DataPacket> data = null;
        for (final long destID : destIDs) {
            final BoltSession session = Optional.ofNullable(serverEndpoint).map(e -> e.getSession(destID)).orElse(null);
            if (session != null) {
                if (data == null) data = codecs.encode(obj);
                for (final DataPacket dp : data) {
                    session.getSocket().doWrite(dp);
                }
            }
        }
    }

    private void shutdown() {
        if (this.serverEndpoint != null) {
            this.serverEndpoint.stop();
            this.serverEndpoint = null;
        }
    }

    public List<BoltStatistics> getStatistics() {
        return serverEndpoint.getSessions().stream().map(BoltSession::getStatistics).collect(Collectors.toList());
    }

    public Config config() {
        return config;
    }

    public CodecRepository codecs() {
        return codecs;
    }

}
