package io.lyracommunity.bolt.api;

/**
 * Generic unchecked Bolt exception.
 *
 * @author Cian.
 */
public class BoltException extends RuntimeException {

    public BoltException(String message) {
        super(message);
    }

    public BoltException(String message, Throwable cause) {
        super(message, cause);
    }

    public BoltException(Throwable cause) {
        super(cause);
    }

}
