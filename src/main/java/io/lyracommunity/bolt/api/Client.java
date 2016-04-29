package io.lyracommunity.bolt.api;

import rx.Observable;

import java.net.InetAddress;

/**
 * Client that is capable of connecting to a remote server.
 *
 * @author Cian.
 */
public interface Client extends Sender {

    /**
     * Connect to a remote server at a particular address and port.
     *
     * @param host the remote host to connect to.
     * @param port the port to connect to.
     * @return the async stream of events for the connection.
     */
    Observable<?> connect(InetAddress host, int port);

}
