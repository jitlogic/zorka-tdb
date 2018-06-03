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


import java.io.File;

import io.zorka.tdb.text.fm.FmIndexFileStore;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import io.zorka.tdb.text.fm.FmTextIndex;
import io.zorka.tdb.text.WalTextIndex;
import org.junit.Test;
import static org.junit.Assert.*;

public class FmTextIndexFileStressTest extends ZicoTestFixture {

    private final int FLIMIT = 64 * MB;

    @Test
    public void testCreateAndCheckFmFile() throws Exception {
        TestStrGen tsg = getSerialStrGen();

        WalTextIndex wal = new WalTextIndex(
                TestUtil.path(tmpDir, "idx1.wal"), 0, FLIMIT);

        long b1 = System.currentTimeMillis();
        while (-1 != wal.add(tsg.get()));
        long b2 = System.currentTimeMillis();

        System.out.println("WAL generating time: " + (b2-b1));

        //System.out.println("Datalen=" + wal.getDatalen());

        String path = TestUtil.path(tmpDir, "idx1.ifm");

        FmIndexFileStoreBuilder builder = new FmIndexFileStoreBuilder(new File(path));

        long t1 = System.currentTimeMillis();
        builder.walToFm(wal);
        long t2 = System.currentTimeMillis();

        System.out.println("Indexing time: " + (t2-t1));

        //mFmIndexTool.print(FmIndexTool.bstats(path), System.out, true);

        // Open as FM index and check if searches work properly

        FmTextIndex fmi = new FmTextIndex(new FmIndexFileStore(path, 0));

        long t3 = System.currentTimeMillis();
        for (int i = 0; i < wal.getNWords(); i++) {
            //System.out.println("id=" + i + ", s=" + wal.gets(i));
            String s0 = wal.gets(i);
            String s1 = fmi.gets(i);
            //mSystem.out.println(s0 + " <-> " + s1);
            assertEquals(s0, s1);
        }
        long t4 = System.currentTimeMillis();

        System.out.println("Check time = " + (t4-t3) + ", nwords=" + wal.getNWords());
    }

}
