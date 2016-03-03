package bolt;

import bolt.packets.DataPacket;
import bolt.xcoder.Server;
import bolt.xcoder.XCoderRepository;
import rx.Observable;
import rx.Subscriber;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Created by omahoc9 on 3/3/16.
 */
public class BoltServer implements Server
{

    private final XCoderRepository xCoderRepository;

    private volatile BoltEndPoint serverEndpoint;

    public BoltServer(final XCoderRepository xCoderRepository)
    {
        this.xCoderRepository = xCoderRepository;
    }

    @Override
    public Observable<?> bind(final InetAddress address, final int port)
    {
        return Observable.create(subscriber -> {
            try
            {
                this.serverEndpoint = new BoltEndPoint(address, port);
                this.serverEndpoint.start(true);
                while (!subscriber.isUnsubscribed()) {
                    pollReceivedData(subscriber);

                    pollNewSessions(subscriber);
                }
            }
            catch (Exception ex) {
                subscriber.onError(ex);
            }
            subscriber.onCompleted();
            shutdown();
        }).share();
    }

    private void pollNewSessions(Subscriber<? super Object> subscriber) {
        final BoltSession session = serverEndpoint.accept();
        if (session != null) {
            CompletableFuture.runAsync()
            // Wait for handshake to complete. TODO what if it doesn't?
            while (!session.isReady() || session.getSocket() == null) {
                Thread.sleep(100);
            }
            return session.getSocket();
        }
    }

    private void pollReceivedData(Subscriber<? super Object> subscriber)
    {
        for (BoltSession session : serverEndpoint.getSessions()) {
            final DataPacket packet = session.getSocket().getReceiveBuffer().poll();

            if (packet != null) {
                // TODO what about classless data.
                final Object decoded = xCoderRepository.decode(packet);
                if (decoded != null) {
                    subscriber.onNext(decoded);
                }
            }
        }
    }

    @Override
    public void send(final Object obj, final long destId) throws IOException
    {
        final BoltSession session = Optional.of(serverEndpoint).map(e -> e.getSession(destId)).orElse(null);
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

    public static class ClientConnected {

        private final long destId;

        public ClientConnected(long destId)
        {
            this.destId = destId;
        }

        public long getDestId()
        {
            return destId;
        }
    }

}
