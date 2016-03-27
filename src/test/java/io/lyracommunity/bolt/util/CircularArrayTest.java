package io.lyracommunity.bolt.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by omahoc9 on 3/24/16.
 */
public class CircularArrayTest {

    @Test
    public void testCircularArray() {
        final CircularArray<Integer> c = new CircularArray<>(5);
        for (int i = 0; i < 5; i++) c.add(i);
        assertEquals(5, c.size());
        c.add(6);
        assertEquals(5, c.size());
        System.out.println(c);
        c.add(7);
        System.out.println(c);
        for (int i = 8; i < 11; i++) c.add(i);
        System.out.println(c);
        c.add(11);
        System.out.println(c);
    }

}