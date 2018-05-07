package io.zorka.tdb.test.unit.util;

import io.zorka.tdb.util.BitmapSet;

import org.junit.Test;
import static org.junit.Assert.*;

public class BitMapSetUnitTest {

    @Test
    public void testAddDelWithoutResize() throws Exception {
        BitmapSet bs = new BitmapSet();

        bs.add(100);
        bs.add(10000);

        assertEquals(2, bs.count());

        assertFalse(bs.get(99));
        assertTrue(bs.get(100));
        assertFalse(bs.get(101));

        assertFalse(bs.get(9999));
        assertTrue(bs.get(10000));
        assertFalse(bs.get(10001));

        bs.add(99);
        bs.add(101);
        bs.del(100);

        assertEquals(3, bs.count());

        assertTrue(bs.get(99));
        assertFalse(bs.get(100));
        assertTrue(bs.get(101));


    }

    // TODO count() method (known to be buggy at the moment)

    // TODO and() method

    // TODO or() method
}

