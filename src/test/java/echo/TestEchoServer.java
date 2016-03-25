package echo;

import bolt.BoltClient;
import bolt.event.ConnectionReadyEvent;
import bolt.receiver.RoutedData;
import bolt.helper.PortUtil;
import junit.framework.Assert;
import org.junit.Test;
import rx.Subscription;
import rx.observables.ConnectableObservable;
import rx.schedulers.Schedulers;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;

public class TestEchoServer {

    @Test
    public void test1() throws Exception {
        final int serverPort = PortUtil.nextServerPort();
        EchoServer es = new EchoServer(serverPort);
        Subscription s = es.start();

        final Set<Object> echoedToClient = new HashSet<>();
        final BoltClient client = new BoltClient(InetAddress.getByName("localhost"), PortUtil.nextClientPort());
        final ConnectableObservable<?> conn = client.connect(InetAddress.getByName("localhost"), serverPort)
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .publish();

        conn.ofType(ConnectionReadyEvent.class)
                .take(1)
                .subscribe(__ -> client.send("test".getBytes()));

        final Subscription cs = conn.ofType(RoutedData.class)
                .ofType(RoutedData.class)
                .take(1)
                .timeout(5, TimeUnit.SECONDS)
                .map(RoutedData::getPayload)
                .subscribe(echoedToClient::add);

        conn.connect();

        while (!cs.isUnsubscribed()) Thread.sleep(10L);

        Assert.assertEquals(1, echoedToClient.size());
        byte[] echoed = (byte[]) echoedToClient.iterator().next();
        assertArrayEquals("test".getBytes(), echoed);
    }

}
