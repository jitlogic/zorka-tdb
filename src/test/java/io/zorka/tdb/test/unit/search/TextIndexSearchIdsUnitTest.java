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

import io.zorka.tdb.search.QueryBuilder;
import io.zorka.tdb.search.SearchNode;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.ci.CompositeIndex;
import io.zorka.tdb.text.ci.CompositeIndexFileStore;
import io.zorka.tdb.text.ci.CompositeIndexStore;
import io.zorka.tdb.util.BitmapSet;
import io.zorka.tdb.util.ZicoUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@RunWith(Parameterized.class)
public class TextIndexSearchIdsUnitTest extends ZicoTestFixture {

    private CompositeIndex idx;

    private boolean archive;

    @Parameterized.Parameters(name="archive={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {false},   // [0] - WAL index
                {true},    // [1] - FM index
        });
    }

    public TextIndexSearchIdsUnitTest(boolean archive) {
        this.archive = archive;
    }

    @Before
    public void createIndex() {
        CompositeIndexStore store = new CompositeIndexFileStore(tmpDir, "test", ZicoUtil.props());
        this.idx = new CompositeIndex(store, ZicoUtil.props(), Runnable::run);
    }

    @After
    public void closeIndex() throws IOException {
        if (idx != null) {
            idx.close();
        }
    }

    @Test @Ignore
    public void testSearchMetaIdsWithLongIds() {
        for (int i = 0; i < 100; i++) {
            idx.add("blop"+i);
        }

        int id1 = idx.add("XYZ");

        if (archive) idx.archive();

        BitmapSet bmps = new BitmapSet();

        SearchNode node = QueryBuilder.stext("XYZ").node();
        int cnt = idx.search(node, bmps);
        assertEquals(1, cnt);
        assertTrue(bmps.get(id1));
    }
}
