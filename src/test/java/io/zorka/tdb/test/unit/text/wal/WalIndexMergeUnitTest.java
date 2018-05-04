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

package io.zorka.tdb.test.unit.text.wal;

import io.zorka.tdb.test.support.ZicoTestFixture;

import java.io.File;

import io.zorka.tdb.text.fm.FmIndexFileStore;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import io.zorka.tdb.text.fm.FmIndexPatchStore;
import io.zorka.tdb.text.fm.FmTextIndex;
import io.zorka.tdb.text.wal.WalTextIndex;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.fm.FmIndexFileStore;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import io.zorka.tdb.text.fm.FmIndexPatchStore;
import io.zorka.tdb.text.fm.FmTextIndex;
import io.zorka.tdb.text.wal.WalTextIndex;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

public class WalIndexMergeUnitTest extends ZicoTestFixture {

    private final static boolean CHECK = true;

    @Test @Ignore // Buggy and unstable, do not use.
    public void testMergeAndQueryFmIndexes() throws Exception {
        File wf1 = new File(tmpDir, "test1.wal");
        WalTextIndex wal1 = new WalTextIndex(wf1, 0, 1024 * 1024);

        int id1 = wal1.add("BROMBA");
        int id2 = wal1.add("OJAAA!");

        File ff1 = new File(tmpDir, "test1.ifm");

        FmIndexFileStoreBuilder fsb1 = new FmIndexFileStoreBuilder(ff1);
        fsb1.walToFm(wal1);
        fsb1.close();
        wal1.close();

        FmIndexFileStore fif1 = new FmIndexFileStore(ff1, 0);

        FmTextIndex idx1 = new FmTextIndex(fif1);
        System.out.println(bwtStr(idx1));


        if (CHECK) {
            assertEquals("BROMBA", idx1.gets(id1));
            assertEquals("OJAAA!", idx1.gets(id2));

            assertEquals(id1, idx1.get("BROMBA"));
            assertEquals(id2, idx1.get("OJAAA!"));
        }

        File wf2 = new File(tmpDir, "test2.wal");

        WalTextIndex wal2 = new WalTextIndex(wf2, 2, 1024 * 1024);

        int id3 = wal2.add("AJWAJ");
        int id4 = wal2.add("OJWOJ");

        FmIndexPatchStore fps2 = new FmIndexPatchStore(fif1);
        fps2.append(wal2);

        File ff2 = new File(tmpDir, "test2.ifm");
        FmIndexFileStoreBuilder fsb2 = new FmIndexFileStoreBuilder(ff2);
        fps2.dump(fsb2, 0, false);
        fsb2.close();

        FmIndexFileStore fif2 = new FmIndexFileStore(ff2, 0);

        FmTextIndex idx2 = new FmTextIndex(fif2);
        System.out.println(bwtStr(idx2));

        if (CHECK) {
            assertEquals("BROMBA", idx2.gets(id1));
            assertEquals("OJAAA!", idx2.gets(id2));
            assertEquals("AJWAJ", idx2.gets(id3));
            assertEquals("OJWOJ", idx2.gets(id4));

            assertEquals(id1, idx2.get("BROMBA"));
            assertEquals(id2, idx2.get("OJAAA!"));
            assertEquals(id3, idx2.get("AJWAJ"));
            assertEquals(id4, idx2.get("OJWOJ"));
        }

        File wf3 = new File(tmpDir, "test3.wal");
        WalTextIndex wal3 = new WalTextIndex(wf3, 4, 1024 * 1024);

        int id5 = wal3.add("CLAZZ");
        int id6 = wal3.add("METHOD");

        FmIndexPatchStore fps3 = new FmIndexPatchStore(fif2);
        fps3.append(wal3);

        File ff3 = new File(tmpDir, "test3.ifm");
        FmIndexFileStoreBuilder fsb3 = new FmIndexFileStoreBuilder(ff3);
        fps3.dump(fsb3, 0, false);
        fsb3.close();

        FmIndexFileStore fif3 = new FmIndexFileStore(ff3, 0);
        FmTextIndex idx3 = new FmTextIndex(fif3);

        System.out.println(bwtStr(idx3));

        if (CHECK) {
            assertEquals("BROMBA", idx3.gets(id1));
            assertEquals("OJAAA!", idx3.gets(id2));
            assertEquals("AJWAJ", idx3.gets(id3));
            assertEquals("OJWOJ", idx3.gets(id4));
            assertEquals("CLAZZ", idx3.gets(id5));
            assertEquals("METHOD", idx3.gets(id6));

            assertEquals(id1, idx3.get("BROMBA"));
            assertEquals(id2, idx3.get("OJAAA!"));
            assertEquals(id3, idx3.get("AJWAJ"));
            assertEquals(id4, idx3.get("OJWOJ"));
            assertEquals(id5, idx3.get("CLAZZ"));
            assertEquals(id6, idx3.get("METHOD"));
        }

        File wf4 = new File(tmpDir, "test4.wal");
        WalTextIndex wal4 = new WalTextIndex(wf4, 6, 1024 * 1024);

        int id7 = wal4.add("KONGO");
        int id8 = wal4.add("BONGO");

        FmIndexPatchStore fps4 = new FmIndexPatchStore(fif3);
        fps4.append(wal4);

        File ff4 = new File(tmpDir, "test4.ifm");
        FmIndexFileStoreBuilder fsb4 = new FmIndexFileStoreBuilder(ff4);
        fps4.dump(fsb4, 0, false);
        fsb4.close();

        FmIndexFileStore fif4 = new FmIndexFileStore(ff4, 0);
        FmTextIndex idx4 = new FmTextIndex(fif4);

        System.out.println(bwtStr(idx4));

        if (CHECK) {
            assertEquals("BROMBA", idx4.gets(id1));
            assertEquals("OJAAA!", idx4.gets(id2));
            assertEquals("AJWAJ", idx4.gets(id3));
            assertEquals("OJWOJ", idx4.gets(id4));
            assertEquals("CLAZZ", idx4.gets(id5));
            assertEquals("METHOD", idx4.gets(id6));
            assertEquals("KONGO", idx4.gets(id7));
            assertEquals("BONGO", idx4.gets(id8));

            assertEquals(id1, idx4.get("BROMBA"));
            assertEquals(id2, idx4.get("OJAAA!"));
            assertEquals(id3, idx4.get("AJWAJ"));
            assertEquals(id4, idx4.get("OJWOJ"));
            assertEquals(id5, idx4.get("CLAZZ"));
            assertEquals(id6, idx4.get("METHOD"));
            assertEquals(id7, idx4.get("KONGO"));
            assertEquals(id8, idx4.get("BONGO"));
        }
    }

    private String bwtStr(FmTextIndex idx3) {
        String s = new String(idx3.extract(idx3.getPidx(), (int) idx3.getDatalen()));
        StringBuilder sb = new StringBuilder(s.length() + 128);

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < 32) {
                sb.append("<").append((int) c).append(">");
            } else {
                sb.append(c);
            }
        }

        sb.append('\n');

        return sb.toString();
    }

    // TODO test na sprawdzanie spójności łączonych stringów

}
