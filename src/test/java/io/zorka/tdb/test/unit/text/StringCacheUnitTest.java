package io.zorka.tdb.test.unit.text;

import io.zorka.tdb.text.StringCache;

import org.junit.Test;
import static org.junit.Assert.*;

public class StringCacheUnitTest {

    @Test
    public void testAddRemoveStrings() {
        StringCache c = new StringCache(4);

        c.add(1, "A");
        c.add(2, "B");
        c.add(3, "C");
        c.add(4, "D");

        assertEquals(1, c.get("A"));
        assertEquals(2, c.get("B"));
        assertEquals(3, c.get("C"));
        assertEquals(4, c.get("D"));

        c.add(5, "E");
        assertEquals(5, c.get("E"));
        assertEquals(-1, c.get("A"));
        assertEquals(2, c.get("B"));

        c.add(6, "F");
        assertEquals(6, c.get("F"));
        assertEquals(-1, c.get("C"));
    }

}
