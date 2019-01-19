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

import io.zorka.tdb.search.QueryBuilder;
import io.zorka.tdb.search.SearchNode;
import io.zorka.tdb.search.TraceSearchQuery;
import io.zorka.tdb.store.*;
import io.zorka.tdb.test.support.ZicoTestFixture;

import java.util.*;

import io.zorka.tdb.test.support.TraceTestDataBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TraceSearchUnitTest extends ZicoTestFixture {

    private boolean archive;


    @Parameterized.Parameters(name="archive={0},indexing={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {false, false},   // [0] - WAL index, scanning search
                {false, true},    // [1] - WAL index, indexing search
                {true, false},    // [2] - FM index, scanning search
                {true, true}      // [3] - FM index, indexing search
        });
    }


    public TraceSearchUnitTest(boolean archive, boolean indexing) {
        this.archive = archive;

        if (indexing) {
            // Force indexing search result
            SimpleTraceStore.SEARCH_QR_THRESHOLD = 0;
            SimpleTraceStore.SEARCH_TX_THRESHOLD = 0;
        } else {
            // Force scanning search result
            SimpleTraceStore.SEARCH_QR_THRESHOLD = Integer.MAX_VALUE;
            SimpleTraceStore.SEARCH_TX_THRESHOLD = Integer.MAX_VALUE;
        }
    }


    @After
    public void resetTraceStoreParams() {
        SimpleTraceStore.SEARCH_QR_THRESHOLD = 512;
        SimpleTraceStore.SEARCH_TX_THRESHOLD = 512;
    }

    private TraceStore store;

    @Before
    public void setupTraceStore() throws Exception {
        store = createAndPopulateStore();
    }

    private RotatingTraceStore createAndPopulateStore() throws Exception {
        RotatingTraceStore store = openRotatingStore();

        String agentUUID = UUID.randomUUID().toString();
        String sessnUUID = store.getSession(agentUUID);

        store.handleAgentData(agentUUID, sessnUUID, TraceTestDataBuilder.agentData());

        store.handleTraceData(agentUUID, sessnUUID, UUID.randomUUID().toString(),
                TraceTestDataBuilder.trc(400, 100, "XXX", "YYY", "AAA", "UVW"),
                md(1, 2));
        store.handleTraceData(agentUUID, sessnUUID, UUID.randomUUID().toString(),
                TraceTestDataBuilder.trc(500, 200, "XXX", "XYZ", "CCC", "UVW"),
                md(1, 2));

        if (archive) store.archive();

        return store;
    }


    @Test
    public void testListAllTracesInArchivedStore() {
        TraceSearchQuery q = QueryBuilder.all().query();
        assertEquals(2, drain(store.searchTraces(q)).size());
    }


    @Test
    public void searchByFreeFormStrings() {
        TraceSearchQuery q = QueryBuilder.stext("XYZ").query();
        assertEquals(1, drain(store.searchTraces(q)).size());
    }


    @Test
    public void searchByFreeFormStringsShallow() {
        TraceSearchQuery q = QueryBuilder.stext("UVW").shallow().query();
        assertEquals(0, drain(store.searchTraces(q)).size());
    }

    @Test
    public void searchByFreeFormStringsDeep() {
        TraceSearchQuery q = QueryBuilder.stext("UVW").deep().query();
        assertEquals(2, drain(store.searchTraces(q)).size());
    }

    @Test
    public void searchByFreeFormStringsPartial() {
        TraceSearchQuery q = QueryBuilder.stext("YZ").query();
        assertEquals(1, drain(store.searchTraces(q)).size());
    }

    @Test
    public void searchByKeyValShallow() {
        TraceSearchQuery q = QueryBuilder.kv("AAA", "UVW").query();  // TODO .shallow()
        assertEquals(1, drain(store.searchTraces(q)).size());
    }

    @Test
    public void searchByKeyValDeep() {
        TraceSearchQuery q = QueryBuilder.kv("AAA", "UVW").query();
        assertEquals(1, drain(store.searchTraces(q)).size());
    }

    // TODO search for exceptions, exception stack traces, stack trace elements

}
