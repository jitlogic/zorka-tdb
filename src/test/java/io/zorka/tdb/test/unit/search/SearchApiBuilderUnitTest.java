package io.zorka.tdb.test.unit.search;

import io.zorka.tdb.search.SortOrder;
import io.zorka.tdb.search.ssn.TextNode;
import org.junit.Test;

import io.zorka.tdb.search.QueryBuilder;

import static org.junit.Assert.*;

/**
 * General cases and API modeling.
 */
public class SearchApiBuilderUnitTest {

    @Test
    public void testSearchTextIndexPhrase() {
        assertTrue(QueryBuilder.xtext("abc").query().getNode() instanceof TextNode);
        assertEquals(100, QueryBuilder.xtext("a").limit(100).query().getLimit());
        assertEquals(50, QueryBuilder.xtext("a").offset(50).query().getOffset());
        assertEquals(20L, QueryBuilder.xtext("a").after(20L).query().getAfter());
        assertEquals(999, QueryBuilder.xtext("a").window(999).query().getWindow());
        assertEquals(SortOrder.CALLS, QueryBuilder.xtext("a").sort(SortOrder.CALLS).query().getSortOrder());
    }


}
