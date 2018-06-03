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
import io.zorka.tdb.store.*;
import io.zorka.tdb.test.support.ZicoTestFixture;

import java.util.List;
import java.util.UUID;

import io.zorka.tdb.store.RecursiveTraceDataRetriever;
import io.zorka.tdb.store.RotatingTraceStore;
import io.zorka.tdb.store.TraceDataIndexer;
import io.zorka.tdb.test.support.TraceTestDataBuilder;
import org.junit.Test;
import static org.junit.Assert.*;

public class SubmitChunkedTraceUnitTest extends ZicoTestFixture {


    @Test
    public void testSubmitRetrieveFragmentedTraceRotatingStore() throws Exception {
        RotatingTraceStore store = openRotatingStore();
        List<String> data = TraceTestDataBuilder.str(
            TraceTestDataBuilder.tr(true, TraceTestDataBuilder.mid(0,0,0), 100, 200, 1,
                TraceTestDataBuilder.ta("XXX", "YYY"),
                TraceTestDataBuilder.tb(1500, 0),
                TraceTestDataBuilder.tr(true, TraceTestDataBuilder.mid(0, 0, 0), 100, 120, 1),
                TraceTestDataBuilder.brk(),
                TraceTestDataBuilder.tr(true, TraceTestDataBuilder.mid(0, 0, 0), 120, 140, 1)
            ));

        assertEquals(2, data.size());


        String agentUUID = UUID.randomUUID().toString();
        String sessnUUID = store.getSession(agentUUID);
        String traceUUID = UUID.randomUUID().toString();

        store.handleAgentData(agentUUID, sessnUUID, TraceTestDataBuilder.agentData());

        ChunkMetadata md = new ChunkMetadata();

        // Send first chunk
        md.setChunkNum(0);
        store.handleTraceData(agentUUID, sessnUUID, traceUUID, data.get(0), md);

        TraceDataIndexer tdi = indexerCache.get(traceUUID);
        assertNotNull(tdi);
        assertEquals(1, tdi.getStackDepth());

        ChunkMetadata md1 = store.getChunkMetadata(store.getChunkIds(traceUUID).get(0));
        assertTrue(0 != (md1.getFlags() & ChunkMetadata.TF_CHUNKED));
        assertTrue(0 != (md1.getFlags() & ChunkMetadata.TF_INITIAL));
        assertTrue(0 == (md1.getFlags() & ChunkMetadata.TF_FINAL));

        RecursiveTraceDataRetriever<TraceRecord> rtr = rtr();
        TraceRecord rslt1 = store.retrieve(traceUUID, rtr);

        assertNotNull(rslt1);
        assertNotNull(rslt1.getChildren());
        assertEquals(1, rslt1.getChildren().size());

        store.archive();
        sessnUUID = store.getSession(agentUUID);
        store.handleAgentData(agentUUID, sessnUUID, TraceTestDataBuilder.agentData());

        // Send second chunk
        md.setChunkNum(1);
        String t1 = data.get(1);
        store.handleTraceData(agentUUID, sessnUUID, traceUUID, t1, md);

        ChunkMetadata md2 = store.getChunkMetadata(store.getChunkIds(traceUUID).get(1));
        assertTrue(0 != (md2.getFlags() & ChunkMetadata.TF_CHUNKED));
        assertTrue(0 == (md2.getFlags() & ChunkMetadata.TF_INITIAL));
        assertTrue(0 != (md2.getFlags() & ChunkMetadata.TF_FINAL));

        rtr.clear();
        TraceRecord rslt2 = store.retrieve(traceUUID, rtr);
        assertNotNull(rslt2);

        assertNotNull(rslt2.getChildren());
        assertEquals(2, rslt2.getChildren().size());
    }

}
