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

package io.zorka.tdb.test.unit.store;

import io.zorka.tdb.meta.MetadataQuickIndex;
import io.zorka.tdb.meta.MetadataSearchQuery;
import io.zorka.tdb.test.support.ZicoTestFixture;

import java.io.File;

import static io.zorka.tdb.store.TraceStoreUtil.*;

import io.zorka.tdb.test.support.ZicoTestFixture;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 */
public class MetadataQuickIndexUnitTest extends ZicoTestFixture {

    @Test
    public void testSimpleAddAndCheck() throws Exception {

        MetadataQuickIndex idx = new MetadataQuickIndex(new File(tmpDir, "test.mqi"));

        idx.add(md(1, 2, 2, 4, 100000, 0,
            false, 100, 0));
        idx.add(md(2, 2, 3, 4, 110000, 0,
            false, 200, 0));
        idx.add(md(3, 2, 3, 4, 120000, 0,
            false, 300, 0));

        assertEquals(3, idx.size());

        MetadataSearchQuery mq = new MetadataSearchQuery();
        mq.setTstart(100000); mq.setTstop(120000);

        assertEquals(3, idx.search(mq).toList().size());

        mq.setAppId(3);
        assertEquals(2, idx.search(mq).toList().size());

        mq.setAppId(2);
        assertEquals(1, idx.search(mq).toList().size());

    }

    @Test
    public void testSimpleAddExtend() throws Exception {

        MetadataQuickIndex idx = new MetadataQuickIndex(new File(tmpDir, "test.mqi"), 128);

        for (int i = 0; i < 8; i++)
            idx.add(md(i % 2, i % 2, i % 3, i % 2, 10000 * (i+10), i % 10,
                false, i * 100, 0));

        assertEquals(8, idx.size());

        MetadataSearchQuery mq = new MetadataSearchQuery();
        mq.setTstart(100000); mq.setTstop(180000);

        assertEquals(8, idx.search(mq).toList().size());
    }


    @Test
    public void testFilterByDuration() throws Exception {
        MetadataQuickIndex idx = new MetadataQuickIndex(new File(tmpDir, "test.mqi"), 128);

        for (int i = 0; i < 40; i++)
            idx.add(md(i % 2, i % 2, i % 3, i % 2, 10000 * (i+10), i % 10,
                false, i * 100, 0));

        assertEquals(40, idx.size());

        MetadataSearchQuery mq = new MetadataSearchQuery();
        mq.setTstart(0); mq.setTstop(Long.MAX_VALUE);

        mq.setDuration(9); assertEquals(4, idx.search(mq).toList().size());
        mq.setDuration(8); assertEquals(8, idx.search(mq).toList().size());
        mq.setDuration(7); assertEquals(12, idx.search(mq).toList().size());
        mq.setDuration(6); assertEquals(16, idx.search(mq).toList().size());
        mq.setDuration(5); assertEquals(20, idx.search(mq).toList().size());
        mq.setDuration(4); assertEquals(24, idx.search(mq).toList().size());
        mq.setDuration(3); assertEquals(28, idx.search(mq).toList().size());
        mq.setDuration(2); assertEquals(32, idx.search(mq).toList().size());
        mq.setDuration(1); assertEquals(36, idx.search(mq).toList().size());
        mq.setDuration(0); assertEquals(40, idx.search(mq).toList().size());
    }

    @Test
    public void testIndexTstampFuzzTolerance() throws Exception {
        MetadataQuickIndex idx = new MetadataQuickIndex(new File(tmpDir, "test.mqi"), 128, 3);

        long[] TS = { 100, 200, 1002, 300, 400, 500, 600, 2003, 700, 800, 1001, 900, 1000, 1100, 1200 };

        for (int i = 0; i < TS.length; i++) {
            idx.add
                (md(i, 1, 1, 1, TS[i],
                    1, false, 1, 0));
        }

        MetadataSearchQuery mq = new MetadataSearchQuery();
        mq.setTstart(1000); mq.setTstop(Long.MAX_VALUE);

        assertEquals(5, idx.search(mq).toList().size());

    }

    @Test
    public void testFormatParseChunkId() {
        long chunkId = formatChunkId(1, 2, true, true);
        assertEquals(1, parseStoreId(chunkId));
        assertEquals(2, parseSlotId(chunkId));
        assertTrue(parseStartFlag(chunkId));
        assertTrue(parseEndFlag(chunkId));
    }

}
