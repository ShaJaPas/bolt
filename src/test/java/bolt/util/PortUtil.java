package bolt.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by keen on 14/03/16.
 */
public class PortUtil {

    private static final AtomicInteger SERVER_PORT = new AtomicInteger(65321);
    private static final AtomicInteger CLIENT_PORT = new AtomicInteger(12345);

    public static int nextServerPort() {
        return SERVER_PORT.getAndIncrement();
    }

    public static int nextClientPort() {
        return CLIENT_PORT.getAndIncrement();
    }

}
