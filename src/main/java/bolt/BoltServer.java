package bolt;

import bolt.packets.DataPacket;
import bolt.receiver.RoutedData;
import bolt.util.Util;
import bolt.xcoder.MessageAssembleBuffer;
import bolt.xcoder.XCoderRepository;
import rx.Observable;
import rx.Subscriber;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;


/**
 * Created by omahoc9 on 3/3/16.
 */
public class BoltServer implements Server {

    private final XCoderRepository xCoderRepository;

    private final Config config;

    private volatile BoltEndPoint serverEndpoint;

    private volatile int count;

    public BoltServer(final Config config) {
        this.xCoderRepository = XCoderRepository.create(new MessageAssembleBuffer());
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
                        final Object decoded = xCoderRepository.decode(packet);
                        if (decoded != null) {
                            subscriber.onNext(new RoutedData(session.getSocketID(), decoded));
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

    @Override
    public void send(final Object obj, final long destId) throws IOException {
        final BoltSession session = Optional.ofNullable(serverEndpoint).map(e -> e.getSession(destId)).orElse(null);
        if (session != null) {
            final Collection<DataPacket> data = xCoderRepository.encode(obj);
            for (final DataPacket dp : data) {
                session.getSocket().doWrite(dp);
            }
        }
    }

    private void shutdown() {
        if (this.serverEndpoint != null) {
            this.serverEndpoint.stop();
            this.serverEndpoint = null;
        }
    }

    public Config config() {
        return config;
    }

    public XCoderRepository xCoderRepository() {
        return xCoderRepository;
    }

}
