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

package io.zorka.tdb.test.unit.store;

import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.meta.MetadataQuickIndex;
import io.zorka.tdb.search.*;
import io.zorka.tdb.test.support.ZicoTestFixture;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static io.zorka.tdb.store.TraceStoreUtil.*;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 */
public class MetadataQuickIndexUnitTest extends ZicoTestFixture {

    private List<Integer> search(MetadataQuickIndex idx, QmiNode query) {
        int[] ids = new int[128], vals = new int[128];

        int cnt = idx.searchBlock(query, SortOrder.NONE, 0, idx.size(), ids, vals);

        List<Integer> rslt = new ArrayList<>(cnt+1);

        for (int i = 0; i < cnt; i++) {
            rslt.add(ids[i]);
        }

        return rslt;
    }

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

        QmiNode q = QmiQueryBuilder.all().tstart(100000).tstop(120000).qmiNode();
        assertEquals(3, search(idx, q).size());

        q.setAppId(3); assertEquals(2, search(idx, q).size());
        q.setAppId(2); assertEquals(1, search(idx, q).size());
    }

    @Test
    public void testSimpleAddExtend() {

        MetadataQuickIndex idx = new MetadataQuickIndex(new File(tmpDir, "test.mqi"), 128);

        for (int i = 0; i < 8; i++)
            idx.add(md(i % 2, i % 2, i % 3, i % 2, 10000 * (i+10), i % 10,
                false, i * 100, 0));

        assertEquals(8, idx.size());

        QmiNode q2 = QmiQueryBuilder.all().tstart(100000).tstop(180000).qmiNode();
        assertEquals(8, search(idx, q2).size());
    }


    @Test
    public void testFilterByDuration() {
        MetadataQuickIndex idx = new MetadataQuickIndex(new File(tmpDir, "test.mqi"), 128);

        for (int i = 0; i < 40; i++)
            idx.add(md(i % 2, i % 2, i % 3, i % 2, 10000 * (i+10), i % 10,
                false, i * 100, 0));

        assertEquals(40, idx.size());

        QmiNode q = QmiQueryBuilder.all().qmiNode();
        q.setMinDuration(9); assertEquals(4, search(idx, q).size());
        q.setMinDuration(8); assertEquals(8, search(idx, q).size());
        q.setMinDuration(7); assertEquals(12, search(idx, q).size());
        q.setMinDuration(6); assertEquals(16, search(idx, q).size());
        q.setMinDuration(5); assertEquals(20, search(idx, q).size());
        q.setMinDuration(4); assertEquals(24, search(idx, q).size());
        q.setMinDuration(3); assertEquals(28, search(idx, q).size());
        q.setMinDuration(2); assertEquals(32, search(idx, q).size());
        q.setMinDuration(1); assertEquals(36, search(idx, q).size());
        q.setMinDuration(0); assertEquals(40, search(idx, q).size());
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

        QmiNode q = QmiQueryBuilder.all().qmiNode();

        q.setHostId(0); assertEquals(40, search(idx, q).size());
        q.setHostId(1); assertEquals(10, search(idx, q).size());
        q.setHostId(2); assertEquals(10, search(idx, q).size());
        q.setHostId(3); assertEquals(10, search(idx, q).size());
    }

    @Test
    public void testFormatParseChunkId() {
        long chunkId = formatChunkId(1, 2, true, true);
        assertEquals(1, parseStoreId(chunkId));
        assertEquals(2, parseSlotId(chunkId));
        assertTrue(parseStartFlag(chunkId));
        assertTrue(parseEndFlag(chunkId));
    }

    @Test
    public void testSearchBlockByBlock() {
        MetadataQuickIndex idx = new MetadataQuickIndex(new File(tmpDir, "test.mqi"), 128);

        for (int i = 0; i < 40; i++) {
            ChunkMetadata cm = md(i % 2, i % 2, i % 3, i % 2, 10000 * (i+10),
                    i % 10, false, i * 100, 0);
            cm.setHostId(i % 4);
            idx.add(cm);
        }

        assertEquals(40, idx.size());

        int[] ids = new int[4], vals = new int[4];

        QmiNode qmi = QmiQueryBuilder.all().qmiNode();

        int cnt1 = idx.searchBlock(qmi, SortOrder.NONE, 0, 4, ids, vals);
        assertEquals(4, cnt1);

        int cnt2 = idx.searchBlock(qmi, SortOrder.NONE, 4, 8, ids, vals);
        assertEquals(4, cnt2);

        int cnt3 = idx.searchBlock(qmi, SortOrder.NONE, 0, 8, ids, vals);
        assertEquals(4, cnt3);
    }

}

