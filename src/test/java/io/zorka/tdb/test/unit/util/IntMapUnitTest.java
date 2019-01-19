/*
 * Copyright 2016-2019 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
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

import io.zorka.tdb.util.IntMap;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Random;

public class IntMapUnitTest {

    // TODO zestaw benchmarków na różne wielkości tablicy i poszukiwanie

    @Test
    public void testSmallMap() {
        Random rnd = new Random();
        int[] keys = new int[1024];

        IntMap imap = new IntMap(64);

        for (int i = 0; i < 64 * 1024; i++) {
            int x = rnd.nextInt(1024);
            if (keys[x] == 0) {
                // new item in map

                while (keys[x] <= 0)
                    keys[x] = rnd.nextInt();

                imap.put(keys[x], keys[x]/2);
            }
            assertEquals(keys[x]/2, imap.get(keys[x]));
        }

    }
}
