package io.lyracommunity.bolt.sender;

import io.lyracommunity.bolt.api.BoltEvent;
import io.lyracommunity.bolt.session.Session;
import io.lyracommunity.bolt.session.SessionController;
import io.lyracommunity.bolt.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.IOException;

/**
 * Thread for processing the {@link Sender} of each session.
 */
public class SenderThread {


    private static final Logger LOG = LoggerFactory.getLogger(SenderThread.class);

    private final SessionController sessions;

    public SenderThread(final SessionController sessions) {
        this.sessions = sessions;
    }

    /**
     * Starts the sender algorithm.
     *
     * @return an async stream of events.
     */
    public Observable<BoltEvent> start() throws IllegalStateException {
        return Observable.create(subscriber -> {
            try {
                Thread.currentThread().setName("Bolt-Sender");

                while (!subscriber.isUnsubscribed()) {
                    long nextStepTime = Long.MAX_VALUE;
                    for (final Session session : sessions.getSessions()) {
                        try {
                            final Sender sender = session.getSender();
                            final long waitUtil = sender.senderAlgorithm();
                            nextStepTime = Math.min(nextStepTime, waitUtil);
                        }
                        catch (IOException | RuntimeException ex) {
                            LOG.error("Unexpected sender IO error", ex);
                            sessions.endSession(subscriber, session.getSessionID(), "Unexpected exception");
                        }
                    }

                    // Wait
                    if (nextStepTime == Long.MAX_VALUE) nextStepTime = Util.currentTimeMicros() + 10_000;
                    final long waitTimeInMillis = (nextStepTime - Util.currentTimeMicros()) / 1000;
                    // Wait if time is sufficiently high, otherwise busy spin.
                    if (waitTimeInMillis > 1) Thread.sleep(waitTimeInMillis);
                    else {
                        Thread.yield();
                        while (Util.currentTimeMicros() < nextStepTime) ; // Busy spin
                    }
                }
            }
            catch (InterruptedException ex) {
                LOG.info("Finished with an interrupt {}", (Object) ex);
            }
            catch (Throwable ex) {
                LOG.error("Unexpected sender thread exception", ex);
                throw new RuntimeException(ex);
            }
            LOG.info("Stopping sender");
            subscriber.onCompleted();
        });
    }

}
