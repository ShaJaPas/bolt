package io.lyracommunity.bolt.api;

import rx.Observable;

import java.io.IOException;

/**
 * Bolt server.
 *
 * @author Cian.
 */
public interface Server extends Sender {


    /**
     * Send a message to all connected clients.
     *
     * @param msg the message to send.
     * @throws IOException if an issue occurred while sending to a client.
     */
    void broadcast(final Object msg) throws IOException;

    /**
     * Begin the server.
     *
     * @return the async event stream.
     */
    Observable<BoltEvent> bind();

    /**
     * Mark a connected client to be disconnected.
     *
     * @param sessionId the client session to disconnect.
     */
    void disconnectClient(int sessionId);


}
