package io.lyracommunity.bolt;

import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by keen on 18/04/16.
 */
public class SessionWorkMediator {

    private final Phaser phaser;

    private int currentPhase = 0;

    public SessionWorkMediator() {
        this.phaser = new Phaser(1);
    }

    public boolean awaitMoreWork(final int timeout, final TimeUnit unit) throws InterruptedException {
        boolean success = true;
        try {
            phaser.awaitAdvanceInterruptibly(currentPhase, timeout, unit);
            currentPhase++;
        }
        catch (TimeoutException e) {
            success = false;
        }
        return success;
    }

    public void workIsAvailable() {
        phaser.arrive();
    }

}
