package io.lyracommunity.bolt.api;

/**
 * Indicates there was an error trying to bind to a UDP port.
 *
 * @author Cian.
 */
public class BoltBindException extends BoltException {

    public BoltBindException(String message) {
        super(message);
    }

    public BoltBindException(String message, Throwable cause) {
        super(message, cause);
    }

    public BoltBindException(Throwable cause) {
        super(cause);
    }

}
