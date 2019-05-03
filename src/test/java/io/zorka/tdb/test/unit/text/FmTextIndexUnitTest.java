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

package io.zorka.tdb.test.unit.text;

import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.WalTextIndex;
import io.zorka.tdb.text.fm.FmTextIndex;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import io.zorka.tdb.text.fm.FmCompressionLevel;
import io.zorka.tdb.text.fm.FmIndexFileStore;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import org.junit.Test;
import static org.junit.Assert.*;

public class FmTextIndexUnitTest extends ZicoTestFixture {

    private final static String[] STRINGS = { "AAA", "BBB", "CCC", "DDD", "EEE" };

    @Test
    public void testCreateAndReadFromFmIndex() throws Exception {
        WalTextIndex wal = new WalTextIndex(TestUtil.path(tmpDir, "idx.wal"), 1, MB);
        Map<Integer,String> strs = new HashMap<>();
        for (String s : STRINGS) {
            int id = wal.add(s);
            assertTrue(id > 0);
            strs.put(id, s);
        }
        String path = TestUtil.path(tmpDir, "idx.ifm");
        FmIndexFileStoreBuilder builder = new FmIndexFileStoreBuilder(new File(path), FmCompressionLevel.LEVEL2);
        builder.walToFm(wal);

        FmTextIndex idx = new FmTextIndex(new FmIndexFileStore(path, 0));
        for (Map.Entry<Integer,String> e : strs.entrySet()) {
            int id = e.getKey();
            String s = e.getValue();

            assertEquals(id, idx.get(s));
            assertEquals(s, idx.gets(id));
        }

        assertEquals(-1, idx.get("XXX"));

        idx.close();
        wal.close();
    }

    @Test
    public void testCreateAndReadEscapedStrings() throws Exception {
        byte[] buf = { 65, 0, 66, 1, 67, 2, 68, 3, 69, -1, 70 };
        WalTextIndex wal = new WalTextIndex(TestUtil.path(tmpDir, "idx.wal"), 1, MB);
        int id1 = wal.add(buf);
        assertNotEquals(-1, id1);

        String path = TestUtil.path(tmpDir, "idx.ifm");
        FmIndexFileStoreBuilder builder = new FmIndexFileStoreBuilder(new File(path), FmCompressionLevel.LEVEL2);
        builder.walToFm(wal);
        FmTextIndex idx = new FmTextIndex(new FmIndexFileStore(path, 0));

        int id2 = idx.get(buf);
        assertEquals(id1, id2);

        byte[] dec = idx.get(id2);

        TestUtil.assertEquals(buf, dec);

        idx.close();
        wal.close();
    }

}
