package echo;

import bolt.BoltClient;
import bolt.event.ConnectionReadyEvent;
import bolt.receiver.RoutedData;
import junit.framework.Assert;
import org.junit.Test;
import rx.Observable;
import rx.Subscription;
import rx.observables.ConnectableObservable;
import rx.schedulers.Schedulers;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TestEchoServer {

    @Test
    public void test1() throws Exception {
        EchoServer es = new EchoServer(65321);
        Subscription s = es.start();
        Thread.sleep(1000);

        final Set<Object> echoedToClient = new HashSet<>();
        final BoltClient client = new BoltClient(InetAddress.getByName("localhost"), 12345);
        final ConnectableObservable<?> conn = client.connect(InetAddress.getByName("localhost"), 65321)
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .publish();

        conn.ofType(ConnectionReadyEvent.class)
                .take(1)
                .subscribe(__ -> client.send("test".getBytes()));

        Subscription cs = conn.ofType(RoutedData.class)
                .take(1)
                .timeout(10, TimeUnit.SECONDS)
                .subscribe(echoedToClient::add);

        conn.connect();

        while (!cs.isUnsubscribed()) Thread.sleep(10L);

        Assert.assertEquals(1, echoedToClient.size());
        Assert.assertEquals("test".getBytes(), echoedToClient.iterator().next());
    }

}
