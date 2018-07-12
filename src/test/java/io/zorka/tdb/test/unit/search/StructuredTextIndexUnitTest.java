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

import io.zorka.tdb.meta.StructuredTextIndex;
import io.zorka.tdb.search.QueryBuilder;
import io.zorka.tdb.search.SearchNode;
import io.zorka.tdb.search.rslt.TextSearchResult;
import io.zorka.tdb.search.ssn.TextNode;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.WalTextIndex;

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
        TextSearchResult sr = idx.search(q);
        assertEquals(1, sr.estimateSize(100));
        long rslt = sr.nextResult();
        assertNotNull(idx.get((int)rslt));
    }

    @Test
    public void testSearchPartialKVPhrases() {
        SearchNode y = QueryBuilder.stext("Y").node();
        SearchNode q = QueryBuilder.kv("A", (TextNode)y).node();
        TextSearchResult sr = idx.search(q);
        assertEquals(2, sr.estimateSize(100));
    }
}
