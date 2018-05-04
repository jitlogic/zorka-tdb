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

import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.fm.FmIndexFileStore;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import io.zorka.tdb.text.fm.FmIndexPatchStore;

import static io.zorka.tdb.text.fm.FmIndexStore.*;


import io.zorka.tdb.text.fm.FmIndexStore;

import java.io.File;
import java.io.IOException;

import io.zorka.tdb.text.fm.FmIndexStore;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

public class FmIndexPatchStoreUnitTest extends ZicoTestFixture {


    private FmIndexFileStore pack(byte[] data) throws IOException {
        File f = new File(tmpDir, "test.ifm");
        FmIndexFileStoreBuilder b = new FmIndexFileStoreBuilder(f);
        b.write(null, data, 0, data.length);
        b.finish(100, 200, 0);
        b.close();

        return new FmIndexFileStore(f, 0);
    }


    @Test
    public void testInsertAndCheckCharsRanks() throws Exception {
        byte[] bf = "ABCDEFGHIJKLMNOPQRSTUWVXYZ".getBytes();
        FmIndexFileStore fif = pack(bf);

        FmIndexPatchStore fip = new FmIndexPatchStore(fif);
        byte[] bb = "ZYXWVUTSRQPONMLKJIHGFEDCBA".getBytes();

        for (int i = bb.length-1; i >= 0; i--) {
            fip.insert(i, bb[i]);
        }

        assertEquals(fip.getDatalen(), bf.length + bb.length);

        checkCharsRanks(bf, bb, fip);

        fif.close();
    }


    @Test
    public void testInsertCharAndDumpNewFmFile() throws Exception {
        byte[] bf = "ABCDEFGHIJKLMNOPQRSTUWVXYZ".getBytes();
        FmIndexFileStore fif = pack(bf);

        FmIndexPatchStore fip = new FmIndexPatchStore(fif);
        byte[] bb = "ZYXWVUTSRQPONMLKJIHGFEDCBA".getBytes();

        for (int i = bb.length-1; i >= 0; i--) {
            fip.insert(i, bb[i]);
        }

        File f1 = new File(tmpDir, "test1.ifm");
        FmIndexFileStoreBuilder ifb = new FmIndexFileStoreBuilder(f1);
        fip.dump(ifb, 0, 16, false);
        ifb.close();

        FmIndexStore fim = new FmIndexFileStore(f1, 0);

        assertEquals(fim.getDatalen(), bf.length + bb.length);

        checkCharsRanks(bf, bb, fim);

        fim.close();
    }



    private void checkCharsRanks(byte[] bf, byte[] bb, FmIndexStore fim) {
        int[] ranks = new int[256];

        for (int i = 0; i < bf.length; i++) {
            int xx = i;
            long cr1 = fim.charAndRank(i*2);
            int c1 = FmIndexStore.chr(cr1) & 0xff;

            Assert.assertEquals("At position " + (i*2), bb[xx], FmIndexStore.chr(cr1));
            Assert.assertEquals("At position " + (i*2), ranks[c1], FmIndexStore.rnk(cr1));

            ranks[c1]++;

            long cr2 = fim.charAndRank(i*2+1);
            int c2 = FmIndexStore.chr(cr2) & 0xff;

            Assert.assertEquals("At position " + (i*2), bf[xx], FmIndexStore.chr(cr2));
            Assert.assertEquals("At position " + (i*2), ranks[c2], FmIndexStore.rnk(cr2));

            ranks[c2]++;

        }
    }

}
