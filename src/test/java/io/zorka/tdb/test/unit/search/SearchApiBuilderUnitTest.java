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
        assertTrue(QueryBuilder.xtext("abc").query(null).getNode() instanceof TextNode);
        assertEquals(100, QueryBuilder.xtext("a").limit(100).query(null).getLimit());
        assertEquals(50, QueryBuilder.xtext("a").offset(50).query(null).getOffset());
        assertEquals(20L, QueryBuilder.xtext("a").after(20L).query(null).getAfter());
        assertEquals(999, QueryBuilder.xtext("a").window(999).query(null).getWindow());
        assertEquals(SortOrder.CALLS, QueryBuilder.xtext("a").sort(SortOrder.CALLS).query(null).getSortOrder());
    }


}
