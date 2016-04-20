package io.lyracommunity.bolt;

import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by keen on 18/04/16.
 */
public class SharedCondition {

    private final Phaser phaser;

    private int currentPhase = 0;

    private final PhaseStrategy strategy;

    public SharedCondition(final PhaseStrategy strategy) {
        this.strategy = strategy;
        this.phaser = new Phaser(1);
    }

    public boolean awaitInterruptibly(final long timeout, final TimeUnit unit) throws InterruptedException {
        boolean success = true;
        try {
            final int nextPhase = phaser.awaitAdvanceInterruptibly(currentPhase, timeout, unit);

            if (strategy == PhaseStrategy.INCREMENT) currentPhase++;
            else if (strategy == PhaseStrategy.LATEST) currentPhase = nextPhase;
        }
        catch (TimeoutException e) {
            success = false;
        }
        return success;
    }

    public void signal() {
        phaser.arrive();
    }


    public enum PhaseStrategy {

        INCREMENT,
        LATEST
    }

}
