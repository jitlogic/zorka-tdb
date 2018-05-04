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

package io.zorka.tdb.test.manual;

import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.text.fm.FmCompressionLevel;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import io.zorka.tdb.text.fm.FmIndexFileStore;

import io.zorka.tdb.text.fm.FmIndexStore;

import static io.zorka.tdb.text.fm.FmIndexStore.chr;

import java.io.IOException;
import java.text.DecimalFormat;

import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import io.zorka.tdb.text.fm.FmIndexStore;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

import static io.zorka.tdb.text.fm.FmIndexTool.*;

import static io.zorka.tdb.test.support.ZicoTestFixture.*;

public class FmTextIndexManualTest {

    @Test
    public void testPackBigFile() throws Exception {
        assumeTrue(T300M_BWT.canRead());
        byte[] bwt = TestUtil.readf(T300M_BWT, 512 * 1024 * 1024, true);
        FmIndexFileStoreBuilder builder = new FmIndexFileStoreBuilder(T300M_IFM);
        builder.bwtToFm(bwt, 0, bwt.length, 100, 200, 0);
        //print(bstats(T300M_IFM), System.out, true);
    }

    @Test
    public void testDisplayBasicInfo() throws Exception {
        assumeTrue(T300M_IFM.canRead());
        print(info(T300M_IFM), System.out, true);
    }

    @Test
    public void testDisplayBlockStats() throws Exception {
        print(bstats(T300M_IFM), System.out, true);
    }

    private final static FmCompressionLevel[] LEVELS = {
            FmCompressionLevel.LEVEL0,
            FmCompressionLevel.LEVEL1,
            FmCompressionLevel.LEVEL2,
            FmCompressionLevel.LEVEL3,
            FmCompressionLevel.LEVEL4
    };

    private final static int[] CSIZES = {
            0,
            1024 * 1024,
            16 * 1024 * 1024,
            64 * 1024 * 1024
    };

    @Test
    public void testCharsAndRanksFromBigFile() throws Exception {
        assumeTrue(T300M_BWT.canRead());
        byte[] bwt = TestUtil.readf(T300M_BWT, 512 * 1024 * 1024, true);
        int[] ranks = new int[256];

        DecimalFormat df = new DecimalFormat("#.00");  // String.format() is such a piece of shit ...


        for (FmCompressionLevel level : LEVELS) {
            for (int csz : CSIZES) {
                pack(bwt, csz, level);
                FmIndexFileStore fif = new FmIndexFileStore(T300M_IFM, FmIndexFileStore.CHECK_ALL_SUMS);

                for (int i = 0; i < ranks.length; i++) {
                    ranks[i] = 0;
                }

                long t1 = System.currentTimeMillis();
                for (int i = 0; i < bwt.length; i++) {
                    try {
                        long car = fif.charAndRank(i);
                        int c1 = bwt[i] & 0xff, c2 = FmIndexStore.chr(car) & 0xff;
                        if (c1 != c2) {
                            long carr = fif.charAndRank(i);
                            System.out.println(carr);
                            assertEquals("Extracted character at index " + i, c1, c2);
                        }
                        long r1 = ranks[c1], r2 = FmIndexStore.rnk(car);
                        if (r1 != r2)
                            assertEquals("Extracted rank at index " + i, r1, r2);
                        ranks[c2]++;
                    } catch (Throwable e) {
                        //System.err.println(e);
                        e.printStackTrace();
                        fail("Crashed at position " + i);
                    }
                }
                long t2 = System.currentTimeMillis();
                long t = t2 - t1;
                long flen = T300M_IFM.length();
                double datapct = 100.0 * flen / bwt.length;
                System.out.println(" -> T=" + df.format(t / 1000.0)
                        + " LEVEL=" + level + " CSZ=" + csz
                        + " BLKSZ=" + level.blksz + " CRMIN=" + level.crmin + " FLEN=" + flen
                        + " PCT=" + df.format(datapct));

                fif.close();
            }
        }
    }


    private void pack(byte[] bwt, int csz, FmCompressionLevel level) throws IOException {
        FmIndexFileStoreBuilder builder = new FmIndexFileStoreBuilder(T300M_IFM, level);
        if (csz == 0) {
            builder.write(null, bwt, 0, bwt.length);
        } else {
            byte[] chunk = new byte[csz];
            for (int pos = 0; pos < bwt.length; pos += csz) {
                int l = Math.min(csz, bwt.length - pos);
                System.arraycopy(bwt, pos, chunk, 0, l);
                builder.write(null, chunk, 0, l);
            }
        }
        builder.finish(100, 200, 0);
        builder.close();
    }

    @Test
    public void testCharAndRankOfSingleChar() throws Exception {
        byte[] bwt = TestUtil.readf(T300M_BWT, 512 * 1024 * 1024, true);
        //FmIndexFileStoreBuilder builder = new FmIndexFileStoreBuilder(T300M_IFM, FmCompressionLevel.LEVEL1);
        //builder.bwtToFm(bwt, 100, 200, 0);
        FmIndexStore fif = new FmIndexFileStore(T300M_IFM, FmIndexFileStore.CHECK_ALL_SUMS);
        long car = fif.charAndRank(4335);
        System.out.println(FmIndexStore.chr(car));
        System.out.println(bwt[4335]);
    }

    @Test
    public void testPureRankInBigFile() throws Exception {
        byte[] bwt = TestUtil.readf(T300M_BWT, 512 * 1024 * 1024, true);
        int[] ranks = new int[256];

        int BRK = 65536, START = 1024 * BRK, STOP = 1152 * BRK;

        for (FmCompressionLevel level : LEVELS) {
            pack(bwt, 1024 * 1024, level);
            System.out.println("********** LEVEL=" + level + " *************");
            FmIndexStore fif = new FmIndexFileStore(T300M_IFM, FmIndexFileStore.CHECK_ALL_SUMS);

            for (int i = 0; i < ranks.length; i++) {
                ranks[i] = 0;
            }

            for (int i = 0; i < START; i++) {
                int c = bwt[i] & 0xff;
                ranks[c]++;
            }

            long t0 = System.currentTimeMillis();

            long t1 = System.currentTimeMillis(), t2;

            for (int i = START; i < STOP; i++) {
                for (int c = 0; c < ranks.length; c++) {
                    int r1 = ranks[c], r2 = fif.rankOf(i, (byte) c);
                    if (r1 != r2) {
                        assertEquals("At chr=" + c + " ('" + (char) c + "')" + " pos=" + i + " (0x" + String.format("%x", i) + ")", r1, r2);
                    }
                }
                int c = bwt[i] & 0xff;
                ranks[c]++;

                if (i != START && i % BRK == BRK - 1) {
                    t2 = System.currentTimeMillis();
                    long rate = BRK * 256L * 1000L / (t2 - t1);
                    System.out.println("CHK: pos=" + i + ", t=" + (t2-t1) + ", rate=" + rate);
                    t1 = t2;
                }
            } // for (int i = START ....)

            long t3 = System.currentTimeMillis();
            long rate = (STOP - START) * 256L * 1000L / (t3 - t0);
            System.out.println("Summary: l=" + level + " t=" + (t3 - t0) + " r=" + rate);
            fif.close();
        }
    }


    @Test
    public void testGetDataFromBigFile() throws Exception {
        byte[] bwt = TestUtil.readf(T300M_BWT, 512 * 1024 * 1024, true);

        for (FmCompressionLevel level : LEVELS) {
            for (int bsz : CSIZES) {

                if (bsz == 0) continue;

                pack(bwt, 0, level);

                byte[] buf = new byte[bsz];

                FmIndexFileStore fif = new FmIndexFileStore(T300M_IFM, FmIndexFileStore.CHECK_ALL_SUMS);

                long t1 = System.currentTimeMillis();
                for (int pos = 0; pos < buf.length; pos += buf.length) {
                    int l = fif.getData(buf, pos);
                    for (int i = 0; i < l; i++) {
                        int xi = i, xp = pos;
                        if (buf[i] != bwt[pos + i]) {
                            assertEquals("At position " + (pos + i), buf[xi], bwt[xp + xi]);
                        }
                    }
                }
                long t2 = System.currentTimeMillis();

                fif.close();

                System.out.println("T=" + (t2 - t1) + " LVL=" + level + " BSZ=" + bsz);
            }
        }
    }


    @Test
    public void testRankOnSinglePosChar() throws Exception {
        int POS = 147, CHR = 9, r1 = 52;
        boolean BUILD = false;

        if (BUILD) {
            byte[] bwt = TestUtil.readf(T300M_BWT, 512 * 1024 * 1024, true);
            FmIndexFileStoreBuilder builder = new FmIndexFileStoreBuilder(T300M_IFM);
            builder.bwtToFm(bwt, 100, 200, 0);
        }

        FmIndexStore fif = new FmIndexFileStore(T300M_IFM, FmIndexFileStore.CHECK_ALL_SUMS);
        int r2 = fif.rankOf(POS, (byte)CHR);
        assertEquals(r1+"=="+r2, r1, r2);
    }

}
