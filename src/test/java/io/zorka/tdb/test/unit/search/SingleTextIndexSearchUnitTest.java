package io.zorka.tdb.test.unit.search;

import io.zorka.tdb.search.QueryBuilder;
import io.zorka.tdb.search.rslt.SearchResult;
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
import io.zorka.tdb.text.wal.WalTextIndex;
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

    private Set<Long> drain(SearchResult sr) {
        Set<Long> rslt = new HashSet<>();
        for (long l = sr.nextResult(); l >= 0; l = sr.nextResult()) {
            rslt.add(l);
        }
        return rslt;
    }

    @Test
    public void testBasicSearch() {
        SearchResult rslt = index.search(QueryBuilder.stext("BA").node());
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

