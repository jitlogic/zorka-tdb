/*
 * Copyright 2016-2018 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
 * <p/>
 * This is free software. You can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * <p/>
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * <p/>
 * You should have received a copy of the GNU General Public License
 * along with this software. If not, see <http://www.gnu.org/licenses/>.
 */

package io.zorka.tdb.test.unit.util;

import io.zorka.tdb.util.BitmapSet;

import org.junit.Test;
import static org.junit.Assert.*;

public class BitMapSetUnitTest {

    @Test
    public void testAddDelWithoutResize() {
        BitmapSet bs = new BitmapSet();

        bs.set(100);
        bs.set(10000);

        assertEquals(2, bs.count());

        assertFalse(bs.get(99));
        assertTrue(bs.get(100));
        assertFalse(bs.get(101));

        assertFalse(bs.get(9999));
        assertTrue(bs.get(10000));
        assertFalse(bs.get(10001));

        bs.set(99);
        bs.set(101);
        bs.del(100);

        assertEquals(3, bs.count());

        assertTrue(bs.get(99));
        assertFalse(bs.get(100));
        assertTrue(bs.get(101));


    }

    @Test
    public void testAndOperator() {
        BitmapSet b1 = new BitmapSet(), b2 = new BitmapSet();

        b1.set(1); b1.set(2); b1.set(3);
        b2.set(2); b2.set(3); b2.set(4);

        BitmapSet ba = b1.and(b2);

        assertFalse(ba.get(1));
        assertTrue(ba.get(2));
        assertTrue(ba.get(3));
        assertFalse(ba.get(4));

        assertEquals(2, ba.count());
    }

    @Test
    public void testOrOperator() {
        BitmapSet b1 = new BitmapSet(), b2 = new BitmapSet();

        b1.set(1); b1.set(2); b1.set(3);
        b2.set(2); b2.set(3); b2.set(4);

        BitmapSet ba = b1.or(b2);

        assertTrue(ba.get(1));
        assertTrue(ba.get(2));
        assertTrue(ba.get(3));
        assertTrue(ba.get(4));

        assertEquals(4, ba.count());
    }

    @Test
    public void testSearchOperator() {
        BitmapSet b = new BitmapSet();

    }
}

