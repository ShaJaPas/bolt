package bolt.cc;

import bolt.BoltCongestionControl;
import bolt.BoltSession;

import java.util.List;

/**
 * Simple TCP CC algorithm from the paper
 * "Optimizing UDP-based Protocol Implementations" by Y. Gu and R. Grossmann
 */
public class SimpleTCP extends BoltCongestionControl {

    public SimpleTCP(BoltSession session) {
        super(session);
    }

    @Override
    public void init() {
        packetSendingPeriod = 0;
        congestionWindowSize = 2;
        setAckInterval(2);
    }

    @Override
    public void onACK(long ackSeqno) {
        congestionWindowSize += 1 / congestionWindowSize;
    }

    @Override
    public void onLoss(List<Integer> lossInfo) {
        congestionWindowSize *= 0.5;
    }


}
