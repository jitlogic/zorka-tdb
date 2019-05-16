package io.zorka.tdb.test.unit.search;

import io.zorka.tdb.store.ChunkMetadata;
import io.zorka.tdb.store.RotatingTraceStore;
import io.zorka.tdb.store.TraceSearchQuery;
import io.zorka.tdb.test.support.TraceTestDataBuilder;
import io.zorka.tdb.test.support.ZicoTestFixture;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static com.jitlogic.zorka.cbor.TraceAttributes.*;
import static com.jitlogic.zorka.common.util.ZorkaUtil.hex;
import static io.zorka.tdb.test.support.TraceTestDataBuilder.*;
import static org.junit.Assert.*;

public class StoreSearchRetrieveUnitTest extends ZicoTestFixture {

    private RotatingTraceStore store;

    @Before
    public void createAndPopulateStore() throws Exception {
        store = openRotatingStore();
        long tid1 = 0x42, tid2 = 0x24;

        String sesId = hex(rand.nextLong());

        store.handleAgentData(sesId, true, TraceTestDataBuilder.agentData());

        store.handleTraceData(sesId, str(
            tr(true, mid(2, 1, 1), 100, 20000, 12345,
                tb(100, 1), ta(COMPONENT, "http"),
                ta(HTTP_METHOD, "GET"), ta(HTTP_STATUS, "200"),
                ta(HTTP_URL, "http://localhost:8642"),
                tr(true, mid(3, 5, 2), 115, 1115, 20,
                    tb(101, 2), ta(COMPONENT, "db"),
                    ta(DB_TYPE, "sql"), ta(DB_STATEMENT, "select 1"),
                    ta(DB_INSTANCE, "test"), ta(DB_USER, "test.user"))
            )).get(0),
            md(tid1, tid2, 0, 0, 0));

        store.handleTraceData(sesId, str(
            tb(102, 3, 1), ta(COMPONENT, "http"),
            ta(HTTP_METHOD, "GET"), ta(HTTP_STATUS, "200"),
            ta(HTTP_URL, "http://localhost:8644")).get(0),
            md(tid1, tid2, 0, 0, 0));
    }

    @Test
    public void testSearchAllChunks() {
        List<ChunkMetadata> lst = store.searchChunks(new TraceSearchQuery(), 10, 0);
        assertEquals(3, lst.size());
    }

    @Test
    public void testCheckParentChildRelationship() {
        List<ChunkMetadata> l = store.searchChunks(new TraceSearchQuery(), 10, 0);
        ChunkMetadata c = bySid(l, 2);
        assertNotNull(c);
        assertEquals(1L, c.getParentId());
    }

    @Test
    public void testSearchAllTraces() {
        TraceSearchQuery q = new TraceSearchQuery();
        Collection<ChunkMetadata> l = store.search(q, 10, 0);
        assertEquals(1, l.size());
        ChunkMetadata c = bySid(l, 1);
        assertNotNull(c);
        assertNotNull(c.getChildren());
        assertEquals(2, c.getChildren().size());
    }

    @Test
    public void testListAttrVals() {
        List<String> lst = store.getAttributeValues(COMPONENT, 100);
        assertEquals(2, lst.size());
        assertTrue(lst.contains("db"));
        assertTrue(lst.contains("http"));
    }

    @Test
    public void testSearchByDuration() {
        TraceSearchQuery q = new TraceSearchQuery().setMinDuration(1000000000L);
        List<ChunkMetadata> lst = store.searchChunks(q, 10, 0);
        assertEquals(1, lst.size());
    }

    @Test
    public void testSearchByAttrs() {
        TraceSearchQuery q = new TraceSearchQuery().attrMatch(COMPONENT, "http");
        List<ChunkMetadata> lst = store.searchChunks(q, 10, 0);
        assertEquals(2, lst.size());
    }

    @Test
    public void testShallowSearch() {
        TraceSearchQuery q = new TraceSearchQuery().attrMatch(COMPONENT, "db");
        List<ChunkMetadata> lst = store.searchChunks(q, 10, 0);
        assertEquals(1, lst.size());
        assertEquals(2L, lst.get(0).getSpanId());
    }

    @Test
    public void searchOnlySpans() {
        TraceSearchQuery q = new TraceSearchQuery().attrMatch(COMPONENT, "db").withSpansOnly();
        List<ChunkMetadata> lst = store.search(q, 10, 0);
        assertEquals(1, lst.size());
        assertEquals(2L, lst.get(0).getSpanId());
    }

    @Test
    public void searchWholeTrees() {
        TraceSearchQuery q = new TraceSearchQuery().attrMatch(COMPONENT, "db");
        List<ChunkMetadata> lst = store.search(q, 10, 0);
        assertEquals(1, lst.size());
        assertEquals(1L, lst.get(0).getSpanId());
        assertEquals(2, lst.get(0).getChildren().size());
    }

    @Test
    public void searchWholeTreesWithNoChildren() {
        TraceSearchQuery q = new TraceSearchQuery().attrMatch(COMPONENT, "db").withNoChildren();
        List<ChunkMetadata> lst = store.search(q, 10, 0);
        assertEquals(1, lst.size());
        assertEquals(1L, lst.get(0).getSpanId());
        assertNull(lst.get(0).getChildren());
        assertTrue(lst.get(0).isHasChildren());
    }

    @Test
    public void searchFreeText() {
        TraceSearchQuery q = new TraceSearchQuery().setText("select 1");
        List<ChunkMetadata> lst = store.searchChunks(q, 10, 0);
        assertEquals(1, lst.size());
        assertEquals(2L, lst.get(0).getSpanId());
    }
}
