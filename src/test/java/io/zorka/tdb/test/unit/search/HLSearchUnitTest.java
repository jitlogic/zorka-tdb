package io.zorka.tdb.test.unit.search;

import io.zorka.tdb.store.SimpleTraceStore;
import io.zorka.tdb.test.support.TraceTestDataBuilder;
import io.zorka.tdb.test.support.ZicoTestFixture;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

@RunWith(Parameterized.class)
public class HLSearchUnitTest extends ZicoTestFixture {

    private boolean archive;
    private SimpleTraceStore store;

    @Parameterized.Parameters(name="archived={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {{false}, {true}
        });
    }

    public HLSearchUnitTest(boolean archive) {
        this.archive = archive;
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
                    md(42L+(i%4), 24L+(i%4), 0, sid, 0));
        }

        if (archive) store.archive();

        return store;
    }

    @Test @Ignore("Fiix after reworking search API")
    public void testSimpleHlSearchDtraceIn() throws Exception {
        store = createAndPopulateStore();

//        QmiNode qmi = new QmiNode();
//
//        qmi.setTraceId1(42L); qmi.setTraceId2(24L);
//        TraceSearchResult tr1 = store.searchTraces(QueryBuilder.all().query(qmi));
//        assertEquals(4, tr1.size());
//        assertFalse(tr1.nextItem().isDtraceOut());
//
//        qmi.setTraceId1(48L);
//        assertEquals(0, store.searchTraces(QueryBuilder.all().query(qmi)).size());
//
//        qmi.setTraceId1(42L);
//        qmi.setSpanId(31L);
//        TraceSearchResult tr2 = store.searchTraces(QueryBuilder.all().query(qmi));
//        assertEquals(1, tr2.size());
//        assertFalse(tr2.nextItem().isDtraceOut());
//
//        qmi.setTraceId1(43L);
//        qmi.setTraceId2(25L);
//        qmi.setSpanId(32L);
//        TraceSearchResult tr3 = store.searchTraces(QueryBuilder.all().query(qmi));
//        assertEquals(1, tr3.size());
//        assertFalse(tr3.nextItem().isDtraceOut());
//
//
//        qmi.setSpanId(28L);
//        assertEquals(0, store.searchTraces(QueryBuilder.all().query(qmi)).size());
    }

    @Test
    public void testSimpleHlSearchAll() throws Exception {
        store = createAndPopulateStore();

//        TraceSearchQuery query = QueryBuilder.all().query();
//        TraceSearchResult rslt = store.searchTraces(query);
//
//        TraceSearchResultItem itm1 = rslt.nextItem();
//        assertNotNull(itm1);
//
//        TraceSearchResultItem itm2 = rslt.nextItem();
//        assertNotNull(itm2);
//
//        assertTrue(itm1.getChunkId() > itm2.getChunkId());
    }

    @Test @Ignore("Fix after reworking search API")
    public void testSimpleHlSearchWithKV() throws Exception {
        store = createAndPopulateStore();

//        TraceSearchQuery query = QueryBuilder.kv("AAA", "UVW5").query(QmiQueryBuilder.all().qmiNode());
//        TraceSearchResult rslt = store.searchTraces(query);
//
//        assertEquals(2, rslt.size());
//
//        TraceSearchResultItem itm1 = rslt.nextItem();
//        assertNotNull(itm1);
//
//        TraceSearchResultItem itm2 = rslt.nextItem();
//        assertNotNull(itm2);
//
//        TraceSearchResultItem itm3 = rslt.nextItem();
//        assertNull(itm3);
    }


    @Test
    public void testSimpleHLSearchWithOffset() throws Exception {
        store = createAndPopulateStore();

//        TraceSearchQuery query = QueryBuilder.all().query();
//        query.setLimit(3); query.setOffset(0);
//
//        TraceSearchResult r1 = store.searchTraces(query);
//        TraceSearchResultItem i11 = r1.nextItem();
//        TraceSearchResultItem i12 = r1.nextItem();
//        TraceSearchResultItem i13 = r1.nextItem();
//
//        query.setOffset(2);
//        TraceSearchResult r2 = store.searchTraces(query);
//        TraceSearchResultItem i21 = r2.nextItem();
//        TraceSearchResultItem i22 = r2.nextItem();
//        TraceSearchResultItem i23 = r2.nextItem();
//
//        assertEquals(i13.getChunkId(), i21.getChunkId());
    }

    // TODO testy na posortowanie
}
