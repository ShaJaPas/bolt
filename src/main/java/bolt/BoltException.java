package bolt;

/**
 * Created by keen on 06/03/16.
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
