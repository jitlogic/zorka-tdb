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

package io.zorka.tdb.test.unit.store;

import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.meta.MetadataQuickIndex;
import io.zorka.tdb.search.QmiNode;
import io.zorka.tdb.search.QueryBuilder;
import io.zorka.tdb.search.SearchNode;
import io.zorka.tdb.test.support.ZicoTestFixture;

import java.io.File;

import static io.zorka.tdb.store.TraceStoreUtil.*;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 */
public class MetadataQuickIndexUnitTest extends ZicoTestFixture {

    @Test
    public void testSimpleAddAndCheck() {

        MetadataQuickIndex idx = new MetadataQuickIndex(new File(tmpDir, "test.mqi"));

        idx.add(md(1, 2, 2, 4, 100000, 0,
            false, 100, 0));
        idx.add(md(2, 2, 3, 4, 110000, 0,
            false, 200, 0));
        idx.add(md(3, 2, 3, 4, 120000, 0,
            false, 300, 0));

        assertEquals(3, idx.size());

        QmiNode q = (QmiNode)QueryBuilder.qmi().tstart(100000).tstop(120000).node();
        assertEquals(3, drain(idx.search(q)).size());

        q.setAppId(3); assertEquals(2, drain(idx.search(q)).size());
        q.setAppId(2); assertEquals(1, drain(idx.search(q)).size());
    }

    @Test
    public void testSimpleAddExtend() {

        MetadataQuickIndex idx = new MetadataQuickIndex(new File(tmpDir, "test.mqi"), 128);

        for (int i = 0; i < 8; i++)
            idx.add(md(i % 2, i % 2, i % 3, i % 2, 10000 * (i+10), i % 10,
                false, i * 100, 0));

        assertEquals(8, idx.size());

        SearchNode q2 = QueryBuilder.qmi().tstart(100000).tstop(180000).node();
        assertEquals(8, drain(idx.search(q2)).size());
    }


    @Test
    public void testFilterByDuration() {
        MetadataQuickIndex idx = new MetadataQuickIndex(new File(tmpDir, "test.mqi"), 128);

        for (int i = 0; i < 40; i++)
            idx.add(md(i % 2, i % 2, i % 3, i % 2, 10000 * (i+10), i % 10,
                false, i * 100, 0));

        assertEquals(40, idx.size());

        QmiNode q = (QmiNode)QueryBuilder.qmi().node();
        q.setMinDuration(9); assertEquals(4, drain(idx.search(q)).size());
        q.setMinDuration(8); assertEquals(8, drain(idx.search(q)).size());
        q.setMinDuration(7); assertEquals(12, drain(idx.search(q)).size());
        q.setMinDuration(6); assertEquals(16, drain(idx.search(q)).size());
        q.setMinDuration(5); assertEquals(20, drain(idx.search(q)).size());
        q.setMinDuration(4); assertEquals(24, drain(idx.search(q)).size());
        q.setMinDuration(3); assertEquals(28, drain(idx.search(q)).size());
        q.setMinDuration(2); assertEquals(32, drain(idx.search(q)).size());
        q.setMinDuration(1); assertEquals(36, drain(idx.search(q)).size());
        q.setMinDuration(0); assertEquals(40, drain(idx.search(q)).size());
    }

    @Test
    public void testFilterByHost() {
        MetadataQuickIndex idx = new MetadataQuickIndex(new File(tmpDir, "test.mqi"), 128);

        for (int i = 0; i < 40; i++) {
            ChunkMetadata cm = md(i % 2, i % 2, i % 3, i % 2, 10000 * (i+10),
                    i % 10, false, i * 100, 0);
            cm.setHostId(i % 4);
            idx.add(cm);
        }

        assertEquals(40, idx.size());

        QmiNode q = (QmiNode)QueryBuilder.qmi().node();

        q.setHostId(0); assertEquals(40, drain(idx.search(q)).size());
        q.setHostId(1); assertEquals(10, drain(idx.search(q)).size());
        q.setHostId(2); assertEquals(10, drain(idx.search(q)).size());
        q.setHostId(3); assertEquals(10, drain(idx.search(q)).size());
    }

    @Test
    public void testIndexTstampFuzzTolerance() {
        MetadataQuickIndex idx = new MetadataQuickIndex(new File(tmpDir, "test.mqi"), 128, 3);

        long[] TS = { 100, 200, 1002, 300, 400, 500, 600, 2003, 700, 800, 1001, 900, 1000, 1100, 1200 };

        for (int i = 0; i < TS.length; i++) {
            idx.add
                (md(i, 1, 1, 1, TS[i],
                    1, false, 1, 0));
        }

        SearchNode q = QueryBuilder.qmi().tstart(1000).node();
        assertEquals(5, drain(idx.search(q)).size());
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
