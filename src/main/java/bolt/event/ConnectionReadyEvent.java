package bolt.event;

import bolt.BoltSession;

/**
 * Created by keen on 03/03/16.
 */
public class ConnectionReadyEvent {

    private final BoltSession sessionReady;

    public ConnectionReadyEvent(BoltSession sessionReady) {
        this.sessionReady = sessionReady;
    }

    public BoltSession getSessionReady() {
        return sessionReady;
    }

}
