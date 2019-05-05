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

package io.zorka.tdb.test.unit.search;

import io.zorka.tdb.store.*;
import io.zorka.tdb.test.support.ZicoTestFixture;

import java.util.*;

import io.zorka.tdb.test.support.TraceTestDataBuilder;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TraceSearchUnitTest extends ZicoTestFixture {

    private boolean archive;


    @Parameterized.Parameters(name="archive={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {false},   // [0] - WAL index, scanning search
                {true}      // [3] - FM index, indexing search
        });
    }


    public TraceSearchUnitTest(boolean archive) {
        this.archive = archive;

    }


    private RotatingTraceStore store;

    @Before
    public void setupTraceStore() throws Exception {
        store = createAndPopulateStore();
    }

    private RotatingTraceStore createAndPopulateStore() throws Exception {
        RotatingTraceStore store = openRotatingStore();

        String sessnUUID = UUID.randomUUID().toString();

        long traceId1 = rand.nextLong(), traceId2 = rand.nextLong();
        long spanId = rand.nextLong();

        store.handleAgentData(sessnUUID, true, TraceTestDataBuilder.agentData());

        store.handleTraceData(sessnUUID,
                TraceTestDataBuilder.trc(1L, 400, 100, "XXX", "YYY", "AAA", "UVW"),
                md(traceId1, traceId2, 0, spanId, 0));
        store.handleTraceData(sessnUUID,
                TraceTestDataBuilder.trc(1L, 500, 200, "XXX", "XYZ", "CCC", "UVW"),
                md(traceId1+1, traceId2+1, 0, spanId+1, 0));

        if (archive) store.rotate();

        return store;
    }


    @Test
    public void testListAllTracesInArchivedStore() {
        TraceSearchQuery query = new TraceSearchQuery();
        List<ChunkMetadata> rslt = store.searchChunks(query, 10, 0);
        assertEquals(2, rslt.size());
    }

    @Test
    public void testListAllTracesWithOffset() {
        TraceSearchQuery query = new TraceSearchQuery();
        List<ChunkMetadata> rslt = store.searchChunks(query, 10, 1);
        assertEquals(1, rslt.size());

    }

    @Test
    public void searchByAttrKV() {
        TraceSearchQuery query = new TraceSearchQuery().attrMatch("XXX", "XYZ");
        List<ChunkMetadata> rslt = store.searchChunks(query, 10, 0);
        assertEquals(1, rslt.size());
    }

    @Test
    public void searchByAttrKVWithOffset() {
        TraceSearchQuery query = new TraceSearchQuery().attrMatch("XXX", "xxx");
        List<ChunkMetadata> rslt = store.searchChunks(query, 10, 0);
        assertEquals(0, rslt.size());
    }



    @Test @Ignore("Fix me")
    public void searchByFreeFormStrings() {
//        TraceSearchQuery q = QueryBuilder.stext("XYZ").query();
//        assertEquals(1, drain(store.searchTraces(q)).size());
    }


    @Test
    public void searchByFreeFormStringsDeep() {
//        TraceSearchQuery q = QueryBuilder.stext("UVW").deep().query();
//        assertEquals(2, drain(store.searchTraces(q)).size());
    }

    @Test @Ignore("Fix me")
    public void searchByFreeFormStringsPartial() {
//        TraceSearchQuery q = QueryBuilder.stext("YZ").query();
//        assertEquals(1, drain(store.searchTraces(q)).size());
    }

    @Test @Ignore("Fix me")
    public void searchByKeyValShallow() {
//        TraceSearchQuery q = QueryBuilder.kv("AAA", "UVW").query();  // TODO .shallow()
//        assertEquals(1, drain(store.searchTraces(q)).size());
    }

    @Test @Ignore("Fix me")
    public void searchByKeyValDeep() {
//        TraceSearchQuery q = QueryBuilder.kv("AAA", "UVW").query();
//        assertEquals(1, drain(store.searchTraces(q)).size());
    }

    // TODO search for exceptions, exception stack traces, stack trace elements

}
