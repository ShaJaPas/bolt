package echo;

import bolt.BoltServer;
import bolt.receiver.RoutedData;
import bolt.xcoder.MessageAssembleBuffer;
import bolt.xcoder.XCoderRepository;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EchoServer {

    final ExecutorService pool = Executors.newFixedThreadPool(2);

    final BoltServer server;

    private final int port;

    volatile boolean started = false;
    volatile boolean stopped = false;

    public EchoServer(final int port) throws Exception {
        this.port = port;
        server = new BoltServer(XCoderRepository.create(new MessageAssembleBuffer()));
    }

    public void stop() {
        stopped = true;
    }

    public synchronized Subscription start() {
        try {
            Subscription s = server.bind(InetAddress.getByName("localhost"), port)
                    .subscribeOn(Schedulers.io())
                    .observeOn(Schedulers.computation())
                    .ofType(RoutedData.class)
//                    .take(1)
                    .subscribe(x -> pool.execute(new Request(server, x)));
            started = true;
            return s;
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public static class Request implements Runnable {

        private final BoltServer server;
        private final RoutedData received;

        public Request(BoltServer server, RoutedData received) {
            this.server = server;
            this.received = received;
        }

        public void run() {
            try {
                if (received.getPayload().getClass().equals(byte[].class)) {
                    System.out.println(new String((byte[]) received.getPayload()));
                }
                else {
                    System.out.println(received.getPayload().toString());
                }

                server.send(received.getPayload(), received.getSourceId());
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

}
