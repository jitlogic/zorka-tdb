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

package io.zorka.tdb.test.unit.search;

import io.zorka.tdb.meta.MetadataSearchQuery;
import io.zorka.tdb.store.*;
import io.zorka.tdb.test.support.ZicoTestFixture;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static io.zorka.tdb.test.support.TraceTestDataBuilder.*;

import io.zorka.tdb.meta.MetadataSearchQuery;
import io.zorka.tdb.store.*;
import io.zorka.tdb.test.support.TraceTestDataBuilder;
import io.zorka.tdb.test.support.ZicoTestFixture;
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


    private List<Long> searchStore(TraceStore store, String...patterns) {
        return searchStore(store, false, patterns);
    }


    private List<Long> searchStore(TraceStore store, boolean deep, String...patterns) {
        StoreSearchQuery query = new StoreSearchQuery();
        query.setLimit(50);
        query.setSPos(0);

        if (deep) query.setSflags(MetadataSearchQuery.DEEP_SEARCH);

        for (String pattern : patterns) { query.addPattern(pattern); }

        TraceSearchResult rslt = store.search(query);
        assertNotNull("TraceStore search result is NULL !", rslt);

        return rslt.drain();
    }


    @Test
    public void testListAllTracesInArchivedStore() {
        assertEquals(2, searchStore(store).size());
    }


    @Test
    public void searchByFreeFormStrings() {
        assertEquals(1, searchStore(store, "XYZ").size());
    }


    @Test
    public void searchByFreeFormStringsShallow() {
        assertEquals(0, searchStore(store, "UVW").size());
    }

    @Test
    public void searchByFreeFormStringsDeep() {
        assertEquals(2, searchStore(store, true, "UVW").size());
    }

    @Test
    public void searchByFreeFormStringsPartial() {
        assertEquals(1, searchStore(store, "YZ").size());
    }

    @Test @Ignore("Bugfix later.")
    public void searchByKeyValShallow() {
        assertEquals(1, searchStore(store, false, "AAA == \"UVW\"").size());
    }

    @Test
    public void searchByKeyValDeep() {
        assertEquals(1, searchStore(store, true, "AAA == \"UVW\"").size());
    }

    // TODO searchByPartialKeyVal()

    // TODO szukanie po parach klucz-wartość;

    // TODO szukanie po regexpach

    // TODO szukanie par po regexach

    // TODO szukanie po wyjątkach

    // TODO szukanie po rekordach stack trace'ów

}
