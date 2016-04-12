package io.lyracommunity.bolt.util;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by keen on 12/04/16.
 */
public class TokenBufferTest {

    private TokenBuffer<SimpleToken> sut;

    @Before
    public void setUp() throws Exception {
        setUp(1000, 1000, 1000);
    }

    private void setUp(final int initialCount, final int tokensPerSec, final int bufferSize) {
        sut = TokenBuffer.perSecond(initialCount, () -> tokensPerSec, bufferSize);
    }

    @Test
    public void offerToFullBuffer_PollReturnsNothing() throws Exception {
        final SimpleToken largeToken = new SimpleToken(1000);
        for (int i = 0; i < 2; i++) sut.offer(largeToken);

        assertEquals(largeToken, sut.poll());
        assertNull(sut.poll());
    }

    @Test
    public void pollEmptyBuffer_NothingReturned() throws Exception {
        assertNull(sut.poll());
    }

    @Test
    public void pollNonEmptyBuffer_AcquireTokensAndReturnItem() throws Exception {
        final SimpleToken t = new SimpleToken(1);
        sut.offer(t);

        assertEquals(t, sut.poll());
    }

    @Test
    public void pollNonEmptyBuffer_NotEnoughTokens() throws Exception {
        final SimpleToken largeToken = new SimpleToken(1000);
        sut.offer(largeToken);
        assertEquals(largeToken, sut.poll());

        sut.offer(largeToken);
        assertNull(sut.poll());
    }

    @Test
    public void offerAndPoll_LongRunning() throws Exception {
        setUp(20, 1000, 2);
        final SimpleToken largeToken = new SimpleToken(1);
        for (int i = 0; i < 50; i++) {
            sut.offer(largeToken);
            Thread.sleep(1);
            assertEquals(largeToken, sut.poll());
        }
        assertNull(sut.poll());
    }

    @Test
    public void offerAndPollInLoopSlowerThanTokenRegen_EachSucceeds() throws Exception {
        setUp(7, 1000, 200);
        long sleepUntil = System.currentTimeMillis() + 5;
        final SimpleToken largeToken = new SimpleToken(5);
        for (int i = 0; i < 20; i++, sleepUntil += 5) {
            assertTrue(sut.offer(largeToken));
            while (System.currentTimeMillis() < sleepUntil) Thread.sleep(1);
            assertEquals(largeToken, sut.poll());
        }
        assertNull(sut.poll());
    }

    @Test
    public void offerAndPollInLoopFasterThanTokenRegen_SomeSucceed() throws Exception {
        final int sleepTime = 10;
        setUp((sleepTime * 3) / 2, 1000, 200);
        long sleepUntil = System.currentTimeMillis() + sleepTime;
        final SimpleToken largeToken = new SimpleToken(sleepTime * 2);
        for (int i = 0; i < 10; i++, sleepUntil += sleepTime) {
            assertTrue(sut.offer(largeToken));
            while (System.currentTimeMillis() < sleepUntil) Thread.sleep(1);
            if (i % 2 == 0) assertEquals(largeToken, sut.poll());
            else assertNull(sut.poll());
        }
        assertNull(sut.poll());
    }

    private static class SimpleToken implements TokenBuffer.Token {
        private final int tokenValue;

        SimpleToken(int tokenValue) {
            this.tokenValue = tokenValue;
        }

        @Override
        public int getLength() {
            return tokenValue;
        }
    }

}