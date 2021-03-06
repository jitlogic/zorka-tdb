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

import io.zorka.tdb.ZicoException;
import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.WalTextIndex;

import io.zorka.tdb.util.QuickHashTab;
import org.junit.After;
import org.junit.Test;


import static org.junit.Assert.*;

public class WalTextIndexUnitTest extends ZicoTestFixture {

    private WalTextIndex idx = null;

    @After
    public void shutDown() throws Exception {
        if (idx != null) {
            idx.close();
            idx = null;
        }
    }

    @Test
    public void testConstructAndCheckHashTab() {
        QuickHashTab ht = new QuickHashTab(1024);
        assertEquals(1024, ht.getMaxSize());
    }

    @Test
    public void testPutGetHashTabData() {
        QuickHashTab ht = new QuickHashTab(1024);

        ht.put(42, 0xdeadbeef, 100);
        ht.put(24, 0xdadacafe, 200);

        assertEquals(100, ht.getById(42));
        assertEquals(200, ht.getById(24));
        assertEquals(100, QuickHashTab.pos(ht.getByHash(0xdeadbeef)));
    }

    @Test(expected = ZicoException.class)
    public void testHashPutInvalidId() {
        QuickHashTab ht = new QuickHashTab(1024);
        ht.put(-1, 2, 3);
    }

    @Test(expected =  ZicoException.class)
    public void testHashGetInvalidId() {
        QuickHashTab ht = new QuickHashTab(1024);
        ht.getById(-1);
    }

    @Test
    public void testHashLockAndPutVal() {
        QuickHashTab ht = new QuickHashTab(1024);
        ht.lock();
        assertFalse(ht.put(1, 2, 3));
    }

    @Test(expected = ZicoException.class)
    public void testOpenWalFileWithIOError() {
        new WalTextIndex("/tmp", 1);
    }

    @Test
    public void testAddAndGetSingleRec() throws Exception {
        idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 1);
        assertEquals(-1L,  idx.get("OJA!"));
        int id = idx.add("OJA!");
        assertNotEquals(-1L, id);
        assertEquals(id, idx.get("OJA!"));
        assertEquals("OJA!", idx.gets(id));
    }

    @Test
    public void testAddAndGetEscapedRec() throws Exception {
        byte[] buf = { 65, 0, 66, 1, 67, 2, 68, 3, 69, -1, 70 };
        idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 1);
        assertEquals(-1L,  idx.get(buf));

        int id = idx.add(buf);
        assertNotEquals(-1L, id);
        assertEquals(id, idx.get(buf));
        TestUtil.assertEquals(buf, idx.get(id));
    }

    @Test
    public void testAddAndGetTwoRecs() throws Exception {
        idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 1);
        long id1 = idx.add("AAA"), id2 = idx.add("BBB");
        assertEquals(id1, idx.get("AAA"));
        assertEquals(id2, idx.get("BBB"));
    }


    @Test
    public void testAddReopenGet() throws Exception {
        idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 1);
        long id1 = idx.add("AAA"), id2 = idx.add("BBB");

        idx.close();

        idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 1);
        assertEquals(id1, idx.get("AAA"));
        assertEquals(id2, idx.get("BBB"));

        long id3 = idx.add("CCC");

        assertEquals(id3, idx.get("CCC"));
        assertNotEquals(id1, id3);
        assertNotEquals(id2, id3);
    }

    @Test
    public void testAddReopenGetWithControlCharacters() throws Exception {
        idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 1);
        long id0 = idx.add("AA\00AA"), id1 = idx.add("BB\01BB"), id2 = idx.add("CC\02CC"), id3 = idx.add("DD\03DD");

        idx.close();

        idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 1);

        assertEquals(id0, idx.get("AA\00AA"));
        assertEquals(id1, idx.get("BB\01BB"));
        assertEquals(id2, idx.get("CC\02CC"));
        assertEquals(id3, idx.get("DD\03DD"));
    }

    @Test
    public void testExtendQuickMap() throws Exception {
        idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 0, 64 * 1024, 4);
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
    }

    @Test
    public void testAccessWalBeforeIdBase() throws Exception {
        WalTextIndex idx = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 1000, 64 * 1024, 4);
        assertNull(idx.get(5));
        assertNull(idx.get(1005));
    }

}
