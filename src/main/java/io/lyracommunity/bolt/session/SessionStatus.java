package io.lyracommunity.bolt.session;

/**
 * Created by keen on 07/04/16.
 */
public enum SessionStatus {
    START(0),
    HANDSHAKING(1),
    HANDSHAKING2(2),
    READY(50),
    SHUTDOWN(90),
    INVALID(99);

    final int seqNo;

    SessionStatus(final int seqNo) {
        this.seqNo = seqNo;
    }

    public int seqNo() {
        return seqNo;
    }
}
