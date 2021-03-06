package io.lyracommunity.bolt.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * helper methods
 */
public class Util {

    public static final AtomicInteger THREAD_INDEX = new AtomicInteger(0);
    private static final long SYN = 10000;
    private static final double SYN_D = 10000.0;

    /**
     * Get the current timer value in microseconds.
     */
    public static long currentTimeMicros() {
        return System.nanoTime() / 1000L;
    }

    /**
     * Get the SYN time in microseconds. The SYN time is 0.01 seconds = 10,000 microseconds.
     */
    public static long getSYNTime() {
        return 10_000L;
    }

    public static double getSYNTimeD() {
        return 10_000.0;
    }

}
