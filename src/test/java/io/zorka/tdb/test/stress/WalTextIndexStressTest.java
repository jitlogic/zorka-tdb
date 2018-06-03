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

package io.zorka.tdb.test.stress;

import io.zorka.tdb.test.support.TestStrGen;
import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.WalTextIndex;

import io.zorka.tdb.util.ZicoUtil;

import java.util.Random;

import org.junit.Test;
import static org.junit.Assert.*;

public class WalTextIndexStressTest extends ZicoTestFixture {

    @Test
    public void testStress() throws Exception {
        TestStrGen tsg = getRandStrGen();
        int NWORDS = 64 * 1024 * 1024, NSTEPS = 64 * 1024 * 1024;
        int[] hashes = new int[NWORDS];
        int[] ids = new int[NWORDS];
        Random rnd = new Random();
        WalTextIndex wal = new WalTextIndex(
                TestUtil.path(tmpDir, "idx1.wal"), 0, 512 * 1024 * 1024);

        for (int x = 0; x < NSTEPS; x++) {
            int i = rnd.nextInt(NWORDS);

            if (hashes[i] == 0) {
                // No record, generate one
                String s = tsg.get();



                //System.out.println("S=" + s);
                byte[] b = s.getBytes();
                hashes[i] = (int)ZicoUtil.crc32(b);
                ids[i] = wal.add(b);
                assertNotEquals(-1, ids[i]);

                // Immediate check
                byte[] b1 = wal.get(ids[i]);
                assertNotNull(b1);
                assertEquals(hashes[i], (int)ZicoUtil.crc32(b1));
                long id1 = wal.get(b);
                assertEquals("At position: " + i + ", s=" + s, ids[i], id1);

            } else {

                // Record exists, check it
                byte[] b = wal.get(ids[i]);
                assertNotNull(b);
                assertEquals(hashes[i], (int)ZicoUtil.crc32(b));
                long id1 = wal.get(b);
                assertEquals("At position: " + i, ids[i], id1);
            }
        }

        System.out.println("Number of strings: " + wal.getNWords());
        System.out.println("Data length: " + wal.getDatalen());
    }

}
