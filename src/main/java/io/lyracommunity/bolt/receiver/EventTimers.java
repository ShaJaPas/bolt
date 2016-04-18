package io.lyracommunity.bolt.receiver;

import io.lyracommunity.bolt.api.Config;
import io.lyracommunity.bolt.util.Util;

/**
 * Created by keen on 18/04/16.
 */
public class EventTimers {


    /**
     * Milliseconds to timeout a new session that stays idle.
     */
    private static final long IDLE_TIMEOUT = 3 * 1000;

    // EXP event related
    /**
     * Instant when the session was created (for expiry checking).
     */
    private final long sessionUpSince;

    /**
     * Record number of consecutive EXP time-out events.
     */
    private volatile long expCount = 0;
    /**
     * to check the ACK, NAK, or EXP timer
     */
    private long nextACK;
    /**
     * Microseconds to next ACK event.
     */
    private long ackTimerInterval = Util.getSYNTime();
    /**
     * Microseconds to next NAK event.
     */
    private long nakTimerInterval = Util.getSYNTime();
    private long nextNAK;
    private long nextEXP;

    private final Config config;


    public EventTimers(final Config config) {
        this.config = config;
        this.sessionUpSince = System.currentTimeMillis();
    }

    void init() {
        nextACK = Util.getCurrentTime() + ackTimerInterval;
        nextNAK = (long) (Util.getCurrentTime() + 1.5 * nakTimerInterval);
        nextEXP = Util.getCurrentTime() + 2 * config.getExpTimerInterval();
    }

    boolean checkIsNextAck(final long currentTimeMicros) {
        final boolean isNextAck = (nextACK < currentTimeMicros);
        if (isNextAck) {
            nextACK = currentTimeMicros + ackTimerInterval;
        }
        return isNextAck;
    }

    boolean checkIsNextNak(final long currentTimeMicros) {
        final boolean isNextNak = (nextNAK < currentTimeMicros);
        if (isNextNak) {
            nextNAK = currentTimeMicros + nakTimerInterval;
        }
        return isNextNak;
    }

    boolean checkIsNextExp(final long currentTimeMicros) {
        final boolean isNextExp = (nextEXP < currentTimeMicros);
        if (isNextExp) {
            nextEXP = currentTimeMicros + config.getExpTimerInterval();
        }
        return isNextExp;
    }

    void updateTimerIntervals(final long roundTripTime, final long roundTripTimeVar) {
        long newAckTimerInterval = 4 * roundTripTime + roundTripTimeVar + Util.getSYNTime();
        if (config.getMaxAckTimerInterval() > 0) {
            newAckTimerInterval = Math.min(config.getMaxAckTimerInterval(), newAckTimerInterval);
        }
        this.ackTimerInterval = newAckTimerInterval;
        this.nakTimerInterval = newAckTimerInterval;
    }

    boolean isSessionExpired() {
        return config.isAllowSessionExpiry()
                && expCount > config.getExpLimit()
                ;
//                && System.currentTimeMillis() - sessionUpSince > IDLE_TIMEOUT;
    }

    void resetEXPTimer() {
        nextEXP = Util.getCurrentTime() + config.getExpTimerInterval();
    }

    void resetEXPCount() {
        expCount = 1;
    }

    long incrementExpCount() {
        return ++expCount;
    }

}
