package io.lyracommunity.bolt.api;

import java.io.IOException;

/**
 * Sends messages to a particular endpoint.
 *
 * @author Cian.
 */
public interface Sender {


    /**
     * Send a message to a particular endpoint.
     *
     * @param msg    the message to send.
     * @param destId the session to send to.
     * @throws IOException if an error occurred while sending the message.
     */
    void send(Object msg, int destId) throws IOException;


}
