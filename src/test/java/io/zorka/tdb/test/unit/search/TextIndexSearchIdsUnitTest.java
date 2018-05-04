package io.zorka.tdb.test.unit.search;

import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.ci.CompositeIndex;
import io.zorka.tdb.text.ci.CompositeIndexFileStore;
import io.zorka.tdb.text.ci.CompositeIndexStore;
import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.util.ZicoUtil;
import io.zorka.tdb.text.ci.CompositeIndexStore;
import io.zorka.tdb.text.re.SearchPattern;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static org.junit.Assert.assertEquals;

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
    public void createIndex() throws IOException {
        CompositeIndexStore store = new CompositeIndexFileStore(tmpDir, "test", ZicoUtil.props());
        this.idx = new CompositeIndex(store, ZicoUtil.props(), Runnable::run);
    }

    @After
    public void closeIndex() throws IOException {
        if (idx != null) {
            idx.close();
        }
    }

    @Test
    public void testSearchMetaIdsWithLongIds() {
        for (int i = 0; i < 100; i++) {
            idx.add("blop"+i);
        }

        Set<Integer> ids1 = ZicoUtil.set(idx.add("XYZ"));

        System.out.println("ARCHIVE = " + archive);
        if (archive) idx.archive();

        Set<Integer> ids2 = idx.searchIds(new SearchPattern("XYZ")).toSet();

        assertEquals(ids1, ids2);
    }
}
