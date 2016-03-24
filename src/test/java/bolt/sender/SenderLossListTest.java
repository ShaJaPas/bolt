package bolt.sender;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by omahoc9 on 3/24/16.
 */
public class SenderLossListTest  {


    @Test
    public void testSenderLossList1() {
        Integer A = 7;
        Integer B = 8;
        Integer C = 1;
        SenderLossList l = new SenderLossList();
        l.insert(A);
        l.insert(B);
        l.insert(C);
        assertEquals(3, l.size());
        Integer oldest = l.getFirstEntry();
        assertEquals(C, oldest);
        oldest = l.getFirstEntry();
        assertEquals(A, oldest);
        oldest = l.getFirstEntry();
        assertEquals(B, oldest);
    }

}