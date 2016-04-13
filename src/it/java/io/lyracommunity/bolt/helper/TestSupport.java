package io.lyracommunity.bolt.helper;

/**
 * Created by keen on 14/04/16.
 */
public class TestSupport {


    public static void sleepUnchecked(final long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
