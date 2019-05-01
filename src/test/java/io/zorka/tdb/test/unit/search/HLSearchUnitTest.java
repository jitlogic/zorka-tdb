package io.zorka.tdb.test.unit.search;

import io.zorka.tdb.search.QmiNode;
import io.zorka.tdb.search.QmiQueryBuilder;
import io.zorka.tdb.search.QueryBuilder;
import io.zorka.tdb.search.TraceSearchQuery;
import io.zorka.tdb.store.SimpleTraceStore;
import io.zorka.tdb.store.TraceSearchResult;
import io.zorka.tdb.store.TraceSearchResultItem;
import io.zorka.tdb.store.TraceStore;
import io.zorka.tdb.test.support.TraceTestDataBuilder;
import io.zorka.tdb.test.support.ZicoTestFixture;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

@RunWith(Parameterized.class)
public class HLSearchUnitTest extends ZicoTestFixture {

    private boolean archive;
    private TraceStore store;

    @Parameterized.Parameters(name="archived={0},indexing={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {false, false},   // [0] - WAL index, scanning search
                {false, true},    // [1] - WAL index, indexing search
                {true, false},    // [2] - FM index, scanning search
                {true, true}      // [3] - FM index, indexing search
        });
    }

    public HLSearchUnitTest(boolean archive, boolean indexing) {
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


    private SimpleTraceStore createAndPopulateStore() throws Exception {
        SimpleTraceStore store = createSimpleStore(1);
        store.open();

        String sessnUUID = UUID.randomUUID().toString();

        store.handleAgentData(sessnUUID, true, TraceTestDataBuilder.agentData());

        for (int i = 0; i < 16; i++) {
            long sid = 31L + i;
            store.handleTraceData(sessnUUID,
                    TraceTestDataBuilder.trc(sid, 100+i*50, 100 + (i*200) % 5000,
                            "XXX", "YYY"+(i % 3), "AAA", "UVW"+(i % 7)),
                    md(42L+(i%4), 24L+(i%4), 0, sid, 0, i%4+1, i%2+1));
        }

        if (archive) store.archive();

        return store;
    }

    @Test
    public void testSimpleHlSearchDtraceIn() throws Exception {
        store = createAndPopulateStore();

        QmiNode qmi = new QmiNode();

        qmi.setTraceId1(42L); qmi.setTraceId2(24L);
        TraceSearchResult tr1 = store.searchTraces(QueryBuilder.all().query(qmi));
        assertEquals(4, tr1.size());
        assertFalse(tr1.nextItem().isDtraceOut());

        qmi.setTraceId1(48L);
        assertEquals(0, store.searchTraces(QueryBuilder.all().query(qmi)).size());

        qmi.setTraceId1(42L);
        qmi.setSpanId(31L);
        TraceSearchResult tr2 = store.searchTraces(QueryBuilder.all().query(qmi));
        assertEquals(1, tr2.size());
        assertFalse(tr2.nextItem().isDtraceOut());

        qmi.setTraceId1(43L);
        qmi.setTraceId2(25L);
        qmi.setSpanId(32L);
        TraceSearchResult tr3 = store.searchTraces(QueryBuilder.all().query(qmi));
        assertEquals(1, tr3.size());
        assertFalse(tr3.nextItem().isDtraceOut());


        qmi.setSpanId(28L);
        assertEquals(0, store.searchTraces(QueryBuilder.all().query(qmi)).size());
    }

    @Test
    public void testSimpleHlSearchAll() throws Exception {
        store = createAndPopulateStore();

        TraceSearchQuery query = QueryBuilder.all().query();
        TraceSearchResult rslt = store.searchTraces(query);

        TraceSearchResultItem itm1 = rslt.nextItem();
        assertNotNull(itm1);

        TraceSearchResultItem itm2 = rslt.nextItem();
        assertNotNull(itm2);

        assertTrue(itm1.getChunkId() > itm2.getChunkId());
    }

    @Test
    public void testSimpleHlSearchWithKV() throws Exception {
        store = createAndPopulateStore();

        TraceSearchQuery query = QueryBuilder.kv("AAA", "UVW5").query(QmiQueryBuilder.all().qmiNode());
        TraceSearchResult rslt = store.searchTraces(query);

        assertEquals(2, rslt.size());

        TraceSearchResultItem itm1 = rslt.nextItem();
        assertNotNull(itm1);

        TraceSearchResultItem itm2 = rslt.nextItem();
        assertNotNull(itm2);

        TraceSearchResultItem itm3 = rslt.nextItem();
        assertNull(itm3);
    }


    @Test
    public void testSimpleHLSearchWithOffset() throws Exception {
        store = createAndPopulateStore();

        TraceSearchQuery query = QueryBuilder.all().query();
        query.setLimit(3); query.setOffset(0);

        TraceSearchResult r1 = store.searchTraces(query);
        TraceSearchResultItem i11 = r1.nextItem();
        TraceSearchResultItem i12 = r1.nextItem();
        TraceSearchResultItem i13 = r1.nextItem();

        query.setOffset(2);
        TraceSearchResult r2 = store.searchTraces(query);
        TraceSearchResultItem i21 = r2.nextItem();
        TraceSearchResultItem i22 = r2.nextItem();
        TraceSearchResultItem i23 = r2.nextItem();

        assertEquals(i13.getChunkId(), i21.getChunkId());
    }

    // TODO testy na posortowanie
}
