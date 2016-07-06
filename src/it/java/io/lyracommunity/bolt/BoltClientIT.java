package io.lyracommunity.bolt;

import io.lyracommunity.bolt.api.Config;
import org.junit.Test;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.net.InetAddress;
import java.util.Random;

/**
 * Client specific tests.
 *
 * @author Cian.
 */
public class BoltClientIT {

    private final Random rnd = new Random();

    @Test
    public void reuseOfAddress() throws Throwable {
        final InetAddress localhostAddr = InetAddress.getByName("localhost");
        final Config clientConfig = new Config(localhostAddr, 12345);

        for (int i = 0; i < 10; i++) {
            final BoltClient client = new BoltClient(clientConfig);
            final Subscription sub = client.connect(localhostAddr, 65432)
                    .subscribeOn(Schedulers.io()).subscribe(x -> {
                    }, Throwable::printStackTrace);

            if (rnd.nextBoolean()) Thread.sleep(1 + rnd.nextInt(100));
            sub.unsubscribe();
        }
    }

}
