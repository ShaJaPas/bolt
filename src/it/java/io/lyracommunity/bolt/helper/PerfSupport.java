package io.lyracommunity.bolt.helper;

import java.util.function.Supplier;

/**
 * Created by keen on 18/04/16.
 */
public class PerfSupport {

    public static void timed(Supplier<Object> exec, int spins) {
        long start = System.nanoTime();
        for (int i = 0; i < spins; i++) {
            exec.get();
        }
        System.out.println("Taken:  " + ((System.nanoTime() - start) / 1_000_000) + "ms");
    }

}
