package io.zorka.tdb.test.unit.search;

import io.zorka.tdb.meta.MetadataTextIndex;
import io.zorka.tdb.search.rslt.SearchResult;
import io.zorka.tdb.test.support.WritableIndexWrapper;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.text.TextIndexType;
import io.zorka.tdb.text.WalTextIndex;
import io.zorka.tdb.text.fm.FmCompressionLevel;
import io.zorka.tdb.text.fm.FmIndexFileStore;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import io.zorka.tdb.text.fm.FmTextIndex;
import io.zorka.tdb.util.ZicoUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static org.junit.Assert.*;

import static io.zorka.tdb.text.TextIndexType.FMI;
import static io.zorka.tdb.text.TextIndexType.WAL;

@RunWith(Parameterized.class)
public class SearchMetaTidsUnitTest extends ZicoTestFixture {

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {FMI},
                {WAL}
        });
    }

    private TextIndexType type;
    private MetadataTextIndex index;

    public SearchMetaTidsUnitTest(TextIndexType type) {
        this.type = type;
    }


    @Before
    public void populateIndex() throws Exception {
        WalTextIndex wal = new WalTextIndex(new File(tmpDir, "test.wal").getPath(), 1);
        MetadataTextIndex mti = new MetadataTextIndex(wal);

        mti.addTextMetaData(11, Arrays.asList(1, 2), false);
        mti.addTextMetaData(11, Arrays.asList(1, 2, 3, 4), true);

        mti.addTextMetaData(12, Arrays.asList(2, 3), false);
        mti.addTextMetaData(12, Arrays.asList(2, 3, 4, 5), true);

        mti.addTextMetaData(13, Arrays.asList(3, 4), false);
        mti.addTextMetaData(13, Arrays.asList(3, 4, 5, 6), true);

        if (type == WAL) {
            this.index = mti;
        } else {
            File fmf = new File(tmpDir, "test.ifm");
            FmIndexFileStoreBuilder builder = new FmIndexFileStoreBuilder(fmf, FmCompressionLevel.LEVEL2);
            builder.walToFm(wal);
            builder.close();
            wal.close();
            TextIndex idx = new FmTextIndex(new FmIndexFileStore(fmf, 0));
            this.index = new MetadataTextIndex(new WritableIndexWrapper(idx, true));
        }
    }


    @Test
    public void testSearchIds() {
        SearchResult sr = index.searchIds(2, false);
        Set<Long> r1 = drain(sr);
        assertEquals(ZicoUtil.set(11L,12L),r1);
    }

}
