/*
 * Copyright 2016-2017 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
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

package io.zorka.tdb.test.unit.text.fm;

import io.zorka.tdb.text.fm.FmPatchNode;
import io.zorka.tdb.text.fm.FmPatchNodeL;
import java.util.Random;
import static io.zorka.tdb.text.fm.FmIndexStore.*;

import io.zorka.tdb.text.fm.FmIndexStore;
import io.zorka.tdb.text.fm.FmPatchNode;
import io.zorka.tdb.text.fm.FmPatchNodeL;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

public class FmPatchNodeUnitTest {

    protected int LENGTH = 4096;
    protected int NSTEPS = 128;
    protected int VERBOSE = 0;

    static Random rand = new Random();

    public FmPatchNode node(int length) {
        return new FmPatchNodeL(length);
    }


    public static long[] genOps(int nitems, int limit) {
        long[] ops = new long[nitems];

        for (int i = 0; i < nitems; i++) {
            ops[i] = FmIndexStore.car((byte)rand.nextInt(256), rand.nextInt(limit));

        }

        return ops;
    }


    public void printOps(long[] ops) {
        StringBuffer sb = new StringBuffer();
        sb.append("= {");
        for (int i = 0; i < ops.length; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(ops[i]);
            sb.append('L');
        }

        sb.append("};");
        System.out.println(sb);
    }



    @Test
    public void testAddOneCharAndCheckProperties() throws Throwable {
            long[] ops = genOps(1024, 4096);
            //printOps(ops);
            testAddOneCharAndCheckProperties(ops, 4096);
    }

    private void testAddOneCharAndCheckProperties(long[] ops, int length) {
        FmPatchNode n = node(length);

        for (int i = 0; i < ops.length; i++) {
            int pos = FmIndexStore.rnk(ops[i]);
            byte ch = FmIndexStore.chr(ops[i]);

            int l1 = n.length(), r1 = n.rankOf(pos, ch), r2 = n.rankOf(pos+1, ch);
            int s1 = n.size(pos), s2 = n.size(pos+1);
            long c1 = n.charAndRank(pos), c2 = n.charAndRank(pos+1);
            //long c1 = n.charAndRank(pos), c2 = n.charAndRank(pos+1);

            if (c1 != -1 && c1 == c2) assertNotEquals("[P=" + pos + ",C=" + ch + "] " + "C[" + i + "]", c1, c2);

            if (VERBOSE > 0) {
                System.out.println("POS=" + pos + ", CH=" + ch + ", I=" + i);
            }

            n = n.insert(pos, ch);

            int l1b = n.length();
            if (l1+1 != l1b) assertEquals("[P=" + pos + ",C=" + ch + "] " + "L1["+i+"]",l1 + 1, l1b);

            int r1b = n.rankOf(pos, ch);
            if (r1 != r1b) assertEquals("[P=" + pos + ",C=" + ch + "] " + "R1["+i+"]", r1, r1b);


            int r2b = n.rankOf(pos + 1, ch);
            if (r2+1 != r2b) {
                long car = n.charAndRank(pos+1);
                if (car == -1 || FmIndexStore.chr(car) != ch) {
                    assertEquals("[P=" + pos + ",C=" + ch + "] " + "R2[" + i + "]",r2 + 1, r2b);
                } else {
                    assertEquals("[P=" + pos + ",C=" + ch + "] " + "R2[" + i + "]", r2, r2b);
                }
            }

            int s1b = n.size(pos);
            if (s1 != s1b) assertEquals("[P=" + pos + ",C=" + ch + "] " + "S1[" + i + "]", s1, s1b);

            if (c1 == -1) {
                int s2b = n.size(pos + 1);
                if (s2+1 != s2b) assertEquals("[P=" + pos + ",C=" + ch + "] " + "S2[" + i + "]",s2+1, s2b);
            } else {
                int s2b = n.size(pos+1);
                if (s2 != s2b) assertEquals("[P=" + pos + ",C=" + ch + "] " + "S2[" + i + "]", s2, s2b);
            }
        }
    }


    @Test
    public void testAddCharAtTheSamePosition() throws Exception {
        byte ch0 = (byte) 10, ch1 = (byte) 11;

        FmPatchNode n = node(LENGTH).insert(10, ch1);

        assertEquals(1, n.rankOf(11, ch1));

        n = n.insert(10, ch0);

        assertEquals(0, n.rankOf(11, ch1));
        assertEquals(1, n.rankOf(12, ch1));
    }

    private final static int MB = 1024 * 1024;

    @Test
    public void testGetRawData() {
        FmPatchNode node = new FmPatchNodeL(4 * MB);

        for (int i = 0; i < MB; i++) {
            node = node.insert(i * 3, (byte)i);
        }

        assertEquals(MB, node.size());

        long[] vals = new long[MB + 1];

        int l = node.getRawData(vals);

        assertEquals(l, MB);

        for (int i = 0; i < MB; i++) {
            Assert.assertEquals(i * 3, FmIndexStore.rnk(vals[i]));
            Assert.assertEquals((byte)i, FmIndexStore.chr(vals[i]));
        }
    }

}
