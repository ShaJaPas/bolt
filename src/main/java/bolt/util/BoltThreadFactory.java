package bolt.util;

import java.text.MessageFormat;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class BoltThreadFactory implements ThreadFactory {

    private static final AtomicInteger num = new AtomicInteger(0);

    private static BoltThreadFactory theInstance = null;

    public static synchronized BoltThreadFactory get() {
        if (theInstance == null) theInstance = new BoltThreadFactory();
        return theInstance;
    }

    public Thread newThread(final Runnable r) {
        return newThread(r, "Thread", false);
    }

    public Thread newThread(final Runnable r, final String threadType, final boolean daemon) {
        final Thread t = new Thread(r);
        t.setName(MessageFormat.format("Bolt-{0}-{1}", threadType, num.incrementAndGet()));
        t.setDaemon(daemon);
        return t;
    }

}
