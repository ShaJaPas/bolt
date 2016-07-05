package io.lyracommunity.bolt.receiver;

import io.lyracommunity.bolt.api.BoltEvent;
import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.session.Session;
import io.lyracommunity.bolt.session.SessionController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Thread for processing the {@link Receiver} of each session.
 */
public class ReceiverThread {


    private static final Logger LOG = LoggerFactory.getLogger(ReceiverThread.class);

    private final SessionController sessions;

    private final Config config;


    public ReceiverThread(final Config config, final SessionController sessions) {
        this.config = config;
        this.sessions = sessions;
    }

    /**
     * Starts the receiver algorithm.
     *
     * @return an async event stream.
     */
    public Observable<BoltEvent> start() {
        return Observable.create(subscriber -> {
            try {
                LOG.info("Starting Receiver Thread");
                Thread.currentThread().setName("Bolt-Receiver");

                while (!subscriber.isUnsubscribed()) {
                    sessions.awaitPacketArrival(computeWaitTimeMicros(), TimeUnit.MICROSECONDS);
                    for (final Session session : sessions.getSessions()) {
                        try {

                            final Receiver receiver = session.getReceiver();
                            final boolean addedData = receiver.receiverAlgorithm(false);

                            if (addedData) {
                                sessions.signalPacketReady();
                            }
                        }
                        catch (final ExpiryException ex) {
                            LOG.warn("Session expired due to timeout");
                            sessions.endSession(subscriber, session.getSessionID(), "Session timeout");
                        }
                        catch (final IOException ex) {
                            LOG.error("Unexpected receiver IO error", ex);
                            sessions.endSession(subscriber, session.getSessionID(), "Unexpected exception");
                        }
                    }
                }
            }
            catch (final InterruptedException ex) {
                LOG.info("Receiver was interrupted.");
            }
            catch (final Exception ex) {
                LOG.error("Unexpected receiver exception", ex);
                subscriber.onError(ex);
            }
            LOG.info("Stopping Receiver");
            subscriber.onCompleted();
        });
    }

    private long computeWaitTimeMicros() {
        final long cappedWait = Math.min(20_000, config.getExpTimerInterval());
        return Math.max(5_000, cappedWait);
    }

}
