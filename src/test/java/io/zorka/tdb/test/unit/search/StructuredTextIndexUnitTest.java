package io.zorka.tdb.test.unit.search;

import io.zorka.tdb.meta.StructuredTextIndex;
import io.zorka.tdb.search.QueryBuilder;
import io.zorka.tdb.search.SearchNode;
import io.zorka.tdb.search.rslt.SearchResult;
import io.zorka.tdb.search.ssn.TextNode;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.wal.WalTextIndex;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class StructuredTextIndexUnitTest extends ZicoTestFixture {

    private WalTextIndex wal;
    private StructuredTextIndex idx;


    @Before
    public void populateIndex() {
        wal = new WalTextIndex(new File(tmpDir, "test.wal").getPath(), 0);
        idx = new StructuredTextIndex(wal);

        idx.addKRPair(idx.add("A"), idx.add("XYZ"));
        idx.addKRPair(idx.add("A"), idx.add("ZYX"));
        idx.addKRPair(idx.add("A"), idx.add("ABC"));
    }


    @After
    public void closeIndex() throws Exception {
        idx.close();
    }


    @Test
    public void testSearchExactKVPhrases() {
        SearchNode q = QueryBuilder.kv("A", "ZYX").node();
        SearchResult sr = idx.search(q);
        assertEquals(1, sr.estimateSize(100));
        long rslt = sr.nextResult();
        assertNotNull(idx.get((int)rslt));
    }

    @Test
    public void testSearchPartialKVPhrases() {
        SearchNode y = QueryBuilder.stext("Y").node();
        SearchNode q = QueryBuilder.kv("A", (TextNode)y).node();
        SearchResult sr = idx.search(q);
        assertEquals(2, sr.estimateSize(100));
    }
}
