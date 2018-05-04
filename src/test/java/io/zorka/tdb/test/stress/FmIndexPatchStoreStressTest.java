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

package io.zorka.tdb.test.stress;

import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.fm.FmIndexFileStore;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import io.zorka.tdb.text.fm.FmIndexPatchStore;

import static io.zorka.tdb.text.fm.FmIndexStore.*;

import io.zorka.tdb.text.fm.FmIndexStore;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import io.zorka.tdb.text.fm.FmIndexPatchStore;
import io.zorka.tdb.text.fm.FmIndexStore;
import org.junit.Test;
import static org.junit.Assert.*;

public class FmIndexPatchStoreStressTest extends ZicoTestFixture {

    private final static int MULT_SZ = 4;

    private final static int INIT_SZ = 65536 * MULT_SZ;
    private final static int PATCH_SZ = 4096 * MULT_SZ;


    @Test
    public void stressInsertCharAndDumpNewFmFile() throws Exception {
        Random rnd = new Random();

        byte[] bf0 = new byte[INIT_SZ];

        for (int i = 0; i < bf0.length; i++) {
            bf0[i] = (byte)(rnd.nextInt(255)+1);
        }

        File f = new File(tmpDir, "test.ifm");

        FmIndexFileStoreBuilder bld0 = new FmIndexFileStoreBuilder(f);
        bld0.write(null, bf0, 0, bf0.length);
        bld0.finish(100, 200, 0);
        bld0.close();

        List<Byte> bl = new ArrayList<>(INIT_SZ+PATCH_SZ);
        for (byte b : bf0) {
            bl.add(b);
        }


        FmIndexFileStore fif = new FmIndexFileStore(f, 0);
        FmIndexPatchStore fip = new FmIndexPatchStore(fif);

        for (int i = 0; i < PATCH_SZ; i++) {
            byte ch = (byte)(rnd.nextInt(255)+1);
            int pos = rnd.nextInt(fip.getDatalen());
            fip.insert(pos, ch);
            bl.add(pos, ch);
        }

        int[] ranks = new int[256];

        check(bl, fip, ranks);

        File f1 = new File(tmpDir, "test1.ifm");
        FmIndexFileStoreBuilder fsb = new FmIndexFileStoreBuilder(f1);
        fip.dump(fsb, 0, false);
        fsb.close();

        fif.close();


        fif = new FmIndexFileStore(f1, 0);
        ranks = new int[256];

        check(bl, fif, ranks);

        fif.close();
    }

    private void check(List<Byte> bl, FmIndexStore fif, int[] ranks) {
        for (int i = 0; i < bl.size(); i++) {
            long car = fif.charAndRank(i);
            byte ch = FmIndexStore.chr(car);
            int rank = FmIndexStore.rnk(car);
            int ci = ch & 0xff;

            if (ch != bl.get(i)) assertEquals("CHR at position " + i, (byte)bl.get(i), ch);
            if (rank != ranks[ci]) assertEquals("RNK ar position " + i, ranks[ci], rank);
            ranks[ci]++;
        }
    }

}
