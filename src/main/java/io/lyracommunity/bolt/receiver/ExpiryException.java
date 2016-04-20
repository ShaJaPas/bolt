package io.lyracommunity.bolt.receiver;

/**
 * Exception signifying a session has expired due
 * to lack of connectivity.
 */
public class ExpiryException extends Exception {

    public ExpiryException(String message) {
        super(message);
    }

}
