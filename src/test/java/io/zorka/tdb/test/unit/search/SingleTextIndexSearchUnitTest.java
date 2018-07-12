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
import io.zorka.tdb.search.rslt.TextSearchResult;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.text.TextIndexType;
import io.zorka.tdb.text.ci.CompositeIndex;
import io.zorka.tdb.text.ci.CompositeIndexFileStore;
import io.zorka.tdb.text.ci.CompositeIndexStore;
import io.zorka.tdb.text.fm.FmCompressionLevel;
import io.zorka.tdb.text.fm.FmIndexFileStore;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import io.zorka.tdb.text.fm.FmTextIndex;
import io.zorka.tdb.text.WalTextIndex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.*;

import static io.zorka.tdb.text.TextIndexType.*;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class SingleTextIndexSearchUnitTest extends ZicoTestFixture {


    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {{FMI}, {WAL}, {COMPOSITE}});
    }

    private TextIndexType type;
    private TextIndex index;
    private String[] samples = { "AA", "BROMBA", "BANGLA", "ABRA" };
    private Map<Long, String> data = new HashMap<>();

    public SingleTextIndexSearchUnitTest(TextIndexType type) {
        this.type = type;
    }


    @Before
    public void populateIndex() throws Exception {
        if (type == WAL || type == FMI) {
            WalTextIndex wal = new WalTextIndex(new File(tmpDir, "test.wal").getPath(), 0);
            for (String s : samples) {
                data.put((long) wal.add(s), s);
            }

            if (type == WAL) {
                // leave as is
                index = wal;
            } else {
                // Compress into FM index
                File fmf = new File(tmpDir, "test.ifm");
                FmIndexFileStoreBuilder builder = new FmIndexFileStoreBuilder(fmf, FmCompressionLevel.LEVEL2);
                builder.walToFm(wal);
                builder.close();
                wal.close();
                index = new FmTextIndex(new FmIndexFileStore(fmf, 0));
            }
        } // type == WAL || type == FMI
        if (type == COMPOSITE) {
            CompositeIndexStore cs = new CompositeIndexFileStore(tmpDir, "test", new Properties());
            CompositeIndex ci = new CompositeIndex(cs, new Properties(), Runnable::run);
            for (String s : samples) {
                data.put((long)ci.add(s), s);
            }
            index = ci;
        }
    }

    @After
    public void closeIndex() throws Exception {
        if (index != null) index.close();
    }


    @Test
    public void testBasicSearch() {
        TextSearchResult rslt = index.search(QueryBuilder.stext("BA").node());
        assertEquals(2, rslt.estimateSize(100));
        Set<Long> ids = drain(rslt);
        assertEquals(2, ids.size());
        for (Long id : ids) {
            //System.out.println(data.get(id));
            assertTrue(data.get(id).contains("BA"));
        }
    }

    // TODO test na matchStart = true

    // TODO test na matchEnd = true
}

