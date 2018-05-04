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

import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.store.*;
import io.zorka.tdb.test.support.ZicoTestFixture;

import java.util.List;
import java.util.UUID;

import static io.zorka.tdb.test.support.TraceTestDataBuilder.*;

import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.store.RotatingTraceStore;
import io.zorka.tdb.store.StoreSearchQuery;
import io.zorka.tdb.store.TraceSearchResult;
import io.zorka.tdb.test.support.TraceTestDataBuilder;
import org.junit.Test;
import static org.junit.Assert.*;

public class SubmitEmbeddedTraceUnitTest extends ZicoTestFixture {

    @Test
    public void testSingleChunkEmbeddedTrace() throws Exception {
        RotatingTraceStore store = openRotatingStore();
        store.open();

        List<String> data = TraceTestDataBuilder.str(
            TraceTestDataBuilder.tr(true, TraceTestDataBuilder.mid(0,0,0), 100, 200, 1,
                TraceTestDataBuilder.ta("XXX", "YYY"),
                TraceTestDataBuilder.tb(1500, 0),
                TraceTestDataBuilder.tr(true, TraceTestDataBuilder.mid(1, 1, 1), 100, 120, 1,
                    TraceTestDataBuilder.tb(1501, 1))
            ));

        String agentUUID = UUID.randomUUID().toString();
        String sessnUUID = store.getSession(agentUUID);
        String traceUUID = UUID.randomUUID().toString();

        store.handleAgentData(agentUUID, sessnUUID, TraceTestDataBuilder.agentData());

        ChunkMetadata md = new ChunkMetadata();

        md.setChunkNum(0);
        store.handleTraceData(agentUUID, sessnUUID, traceUUID, data.get(0), md);

        StoreSearchQuery query = new StoreSearchQuery();

        query.setLimit(50);query.setSPos(0);
        TraceSearchResult sr = store.search(query);

        assertNotNull(sr);
        List<Long> lst = sr.drain();
        assertEquals(2, lst.size());

        ChunkMetadata md1 = store.getChunkMetadata(lst.get(0));
        //assertEquals(0, md1.getTypeId());  TODO popsuło się przy implementacji translacji TID, uwzględnić przy ponownym uruchamianiu tej funkcjonalności
        assertEquals(0, md1.getStartOffs());

        ChunkMetadata md2 = store.getChunkMetadata(lst.get(1));
        assertEquals(1, md2.getTypeId());
        assertTrue(md2.getStartOffs() != 0);


        String uuid1 = store.getTraceUUID(lst.get(0));
        assertEquals(traceUUID, uuid1);

        TraceRecord rslt1 = store.retrieve(uuid1, rtr());
        assertNotNull(rslt1);
        assertNotNull(rslt1.getChildren());
        assertEquals(1, rslt1.getChildren().size());


        String uuid2 = store.getTraceUUID(lst.get(1));
        assertNotEquals(traceUUID, uuid2);

        TraceRecord rslt2 = store.retrieve(uuid2, rtr());
        assertNotNull(rslt2);
        assertNull(rslt2.getChildren());
    }

    @Test
    public void testMultipleChunkEmbeddedTrace() throws Exception {
        RotatingTraceStore store = openRotatingStore();
        store.open();

        List<String> data = TraceTestDataBuilder.str(
            TraceTestDataBuilder.tr(true, TraceTestDataBuilder.mid(0,0,0), 100, 200, 1,
                TraceTestDataBuilder.ta("XXX", "YYY"),
                TraceTestDataBuilder.tb(1500, 0),
                TraceTestDataBuilder.brk(),
                TraceTestDataBuilder.ta("ZZZ", "VVV"),
                TraceTestDataBuilder.tr(true, TraceTestDataBuilder.mid(1, 1, 1), 100, 120, 1,
                    TraceTestDataBuilder.tb(1501, 1))

            ));

        String agentUUID = UUID.randomUUID().toString();
        String sessnUUID = store.getSession(agentUUID);
        String traceUUID = UUID.randomUUID().toString();

        store.handleAgentData(agentUUID, sessnUUID, TraceTestDataBuilder.agentData());

        ChunkMetadata md = new ChunkMetadata();

        md.setChunkNum(0);
        store.handleTraceData(agentUUID, sessnUUID, traceUUID, data.get(0), md);

        store.archive();

        sessnUUID = store.getSession(agentUUID);
        store.handleAgentData(agentUUID, sessnUUID, TraceTestDataBuilder.agentData());

        md.setChunkNum(1);
        store.handleTraceData(agentUUID, sessnUUID, traceUUID, data.get(1), md);

        StoreSearchQuery query = new StoreSearchQuery();

        query.setLimit(50);query.setSPos(0);
        TraceSearchResult sr = store.search(query);

        assertNotNull(sr);
        List<Long> lst = sr.drain();
        assertEquals(3, lst.size());

        ChunkMetadata md1 = store.getChunkMetadata(lst.get(0));
        //assertEquals(0, md1.getTypeId()); TODO popsuło się po wprowadzeniu transkacji typeId, uwzględnić przy ponownym uruchamianiu
        assertEquals(0, md1.getStartOffs());
        assertNotEquals(0, md1.getMethodId());

        ChunkMetadata md2 = store.getChunkMetadata(lst.get(1));
        assertEquals(1, md2.getTypeId());
        assertTrue(md2.getStartOffs() != 0);
        assertNotEquals(0, md2.getMethodId());

        String uuid1 = store.getTraceUUID(lst.get(0));
        assertEquals(traceUUID, uuid1);

        TraceRecord rslt1 = store.retrieve(uuid1, rtr());
        assertNotNull(rslt1);
        assertNotNull(rslt1.getChildren());
        assertEquals(1, rslt1.getChildren().size());


        String uuid2 = store.getTraceUUID(lst.get(1));
        assertNotEquals(traceUUID, uuid2);

        TraceRecord rslt2 = store.retrieve(uuid2, rtr());
        assertNotNull(rslt2);
        assertNull(rslt2.getChildren());

    }

}
