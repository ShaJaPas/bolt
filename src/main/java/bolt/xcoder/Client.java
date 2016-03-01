package bolt.xcoder;

import rx.Observable;

import java.net.InetAddress;

/**
 * Created by keen on 28/02/16.
 */
public interface Client extends Sender {

    Observable<?> connect(InetAddress host, int port);

}
