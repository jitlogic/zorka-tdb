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

package io.zorka.tdb.test.integ;

import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.fm.FmCompressionLevel;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import io.zorka.tdb.text.fm.FmIndexFileStore;
import io.zorka.tdb.text.fm.FmIndexStore;

import java.io.File;
import java.io.IOException;

import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.fm.FmCompressionLevel;
import io.zorka.tdb.text.fm.FmIndexFileStore;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import io.zorka.tdb.text.fm.FmIndexStore;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class FmTextIndexIntegTest extends ZicoTestFixture {

    int fsize = 1 * MB;

    File strf = new File(tmpDir, "test.str");

    byte[] bwt;

    @Before
    public void prepareBwtFile() throws Exception {
        if (!strf.canRead()) {
            prepareStrings(strf, fsize);
        }

        File bwtf = new File(tmpDir,"test.bwt");
        if (!bwtf.canRead()) {
            byte[] buf = TestUtil.readf(strf.getPath(), fsize, false);
            bwt = FmIndexFileStoreBuilder.walToBwt(buf);
            TestUtil.writef(bwtf.getPath(), bwt);
        } else {
            bwt = TestUtil.readf(bwtf.getPath(), fsize, false);
        }
    }

    @Test
    void testIndexFileLevel0() throws Exception {
        testFmIndex(FmCompressionLevel.LEVEL0);
    }

    @Test
    void testIndexFileLevel1() throws Exception {
        testFmIndex(FmCompressionLevel.LEVEL1);
    }

    @Test
    void testIndexFileLevel2() throws Exception {
        testFmIndex(FmCompressionLevel.LEVEL2);
    }

    @Test
    void testIndexFileLevel3() throws Exception {
        testFmIndex(FmCompressionLevel.LEVEL3);
    }

    @Test
    void testIndexFileLevel4() throws Exception {
        testFmIndex(FmCompressionLevel.LEVEL4);
    }

    private void testFmIndex(FmCompressionLevel lvl) throws IOException {
        File f = new File(tmpDir, "test0.ifm");
        FmIndexFileStoreBuilder builder = new FmIndexFileStoreBuilder(f, lvl);
        builder.bwtToFm(bwt, 0, bwt.length, 100, 200, 100);

        FmIndexFileStore fif = new FmIndexFileStore(f, 0);
        int[] ranks = new int[256];

        for (int i = 0; i < fsize; i++) {
            long car = fif.charAndRank(i);

            byte c1 = bwt[i], c2 = FmIndexStore.chr(car);

            if (c2 != c1) {
                fail("Bad char at position " + i + ": " + c1 + " <-> " + c2);
            }

            int r1 = ranks[c2 & 0xff], r2 = FmIndexStore.rnk(car);

            if (r2 != r1) {
                fail("Bad rank at position " + i + ": " + r1 + " <-> " + r2);
            }

            ranks[c2 & 0xff]++;
        }

        fif.close();
    }

}
