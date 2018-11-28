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

package io.zorka.tdb.test.unit.text.wal;

import io.zorka.tdb.search.ssn.TextNode;
import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.WalTextIndex;

import io.zorka.tdb.util.BitmapSet;
import org.junit.Test;
import static org.junit.Assert.*;

public class WalTextIndexUnitTest extends ZicoTestFixture {

    @Test
    public void testAddAndGetSingleRec() throws Exception {
        WalTextIndex idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 1);
        assertEquals(-1L,  idx.get("OJA!"));
        int id = idx.add("OJA!");
        assertNotEquals(-1L, id);
        assertEquals(id, idx.get("OJA!"));
        assertEquals("OJA!", idx.gets(id));

        idx.close();
    }

    @Test
    public void testAddAndGetEscapedRec() throws Exception {
        byte[] buf = { 65, 0, 66, 1, 67, 2, 68, 3, 69, -1, 70 };
        WalTextIndex idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 1);
        assertEquals(-1L,  idx.get(buf));

        int id = idx.add(buf);
        assertNotEquals(-1L, id);
        assertEquals(id, idx.get(buf));
        TestUtil.assertEquals(buf, idx.get(id));

        idx.close();

    }

    @Test
    public void testAddAndGetTwoRecs() throws Exception {
        WalTextIndex idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 1);
        long id1 = idx.add("AAA"), id2 = idx.add("BBB");
        assertEquals(id1, idx.get("AAA"));
        assertEquals(id2, idx.get("BBB"));

        idx.close();
    }


    @Test
    public void testAddReopenGet() throws Exception {
        WalTextIndex idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 1);
        long id1 = idx.add("AAA"), id2 = idx.add("BBB");

        idx.close();

        idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 1);
        assertEquals(id1, idx.get("AAA"));
        assertEquals(id2, idx.get("BBB"));

        long id3 = idx.add("CCC");

        assertEquals(id3, idx.get("CCC"));
        assertNotEquals(id1, id3);
        assertNotEquals(id2, id3);

        idx.close();
    }

    @Test
    public void testAddReopenGetWithControlCharacters() throws Exception {
        WalTextIndex idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 1);
        long id0 = idx.add("AA\00AA"), id1 = idx.add("BB\01BB"), id2 = idx.add("CC\02CC"), id3 = idx.add("DD\03DD");

        idx.close();

        idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 1);

        assertEquals(id0, idx.get("AA\00AA"));
        assertEquals(id1, idx.get("BB\01BB"));
        assertEquals(id2, idx.get("CC\02CC"));
        assertEquals(id3, idx.get("DD\03DD"));

        idx.close();
    }

    @Test
    public void testExtendQuickMap() throws Exception {
        WalTextIndex idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 0, 64 * 1024, 4);
        String[] str = new String[] { "AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH", "III", "JJJ" };
        int[] ids = new int[str.length];

        int LEN = 5;

        for (int i = 0; i < LEN; i++) {
            ids[i] = idx.add(str[i].getBytes());
            assertNotEquals(-1, ids[i]);
        }

        for (int i = 0; i < LEN; i++) {
            byte[] b = idx.get(ids[i]);
            final int xx = i;

            assertNotNull("At i = " + i, b);
            assertEquals("At i = " + i, str[xx], new String(b));
            assertEquals("At i = " + i, ids[xx], idx.get(str[xx].getBytes()));
        }

        idx.close();
    }

    @Test
    public void testAccessWalBeforeIdBase() throws Exception {
        WalTextIndex idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 1000, 64 * 1024, 4);
        assertNull(idx.get(5));
        assertNull(idx.get(1005));
    }

    @Test
    public void testSimpleSearchWalIndex() throws Exception {
        // Only simple search to ensure that IDs map properly
        WalTextIndex idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 0, 64 * 1024, 4);
        String[] str = new String[] { "AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH", "III", "JJJ" };
        int[] ids = new int[str.length];

        for (int i = 0; i < str.length; i++) {
            ids[i] = idx.add(str[i].getBytes());
            assertNotEquals(-1, ids[i]);
        }

        BitmapSet bbs = new BitmapSet();
        int cnt = idx.search(new TextNode("C", false, false), bbs);

        assertEquals(1, cnt);
        assertTrue(bbs.get(ids[2]));

        idx.close();
    }

    // TODO test searchIds()
}
