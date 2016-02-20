package bolt.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class BoltThreadFactory implements ThreadFactory {

    private static final AtomicInteger num = new AtomicInteger(0);

    private static BoltThreadFactory theInstance = null;

    public static synchronized BoltThreadFactory get() {
        if (theInstance == null) theInstance = new BoltThreadFactory();
        return theInstance;
    }

    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName("Bolt-Thread-" + num.incrementAndGet());
        return t;
    }

}
