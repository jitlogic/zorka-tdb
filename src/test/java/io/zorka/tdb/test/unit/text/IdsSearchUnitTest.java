package io.zorka.tdb.test.unit.text;

import io.zorka.tdb.meta.MetadataTextIndex;
import io.zorka.tdb.text.TextIndexType;
import io.zorka.tdb.text.WritableTextIndex;
import io.zorka.tdb.util.BitmapSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/** Test searchIds() method on various types of text index. */
@RunWith(Parameterized.class)
public class IdsSearchUnitTest extends TextIndexTestFixture {

    public IdsSearchUnitTest(TextIndexType type) {
        super(type);
    }

    private void addData(boolean deep, WritableTextIndex wal) {
        MetadataTextIndex mix = new MetadataTextIndex(wal);
        mix.addTextMetaData(42, Arrays.asList(11, 22, 33), deep);
        mix.addTextMetaData(24, Arrays.asList(22, 33, 44), deep);
        mix.addTextMetaData(84, Arrays.asList(33, 44, 55), deep);
    }


    @Test
    public void testSearchIdsShallow() throws Exception {
        newIndex(x -> addData(false, x));

        BitmapSet bbs = new BitmapSet();
        int cnt11 = idx.searchIds(11, false, bbs);
        assertEquals(1, cnt11);
        assertTrue(bbs.get(42));

        assertEquals(0, idx.searchIds(11, true, bbs));

        int cnt22 = idx.searchIds(22, false, bbs);
        assertEquals(2, cnt22);
        assertTrue(bbs.get(42));
        assertTrue(bbs.get(24));
        assertFalse(bbs.get(84));

        assertEquals(0, idx.searchIds(22, true, bbs));
    }


    @Test
    public void testSearchIdsDeep() throws Exception {
        newIndex(x -> addData(true, x));

        BitmapSet bbs = new BitmapSet();
        int cnt11 = idx.searchIds(11, true, bbs);
        assertEquals(1, cnt11);
        assertTrue(bbs.get(42));

        assertEquals(0, idx.searchIds(11, false, bbs));

        int cnt22 = idx.searchIds(22, true, bbs);
        assertEquals(2, cnt22);
        assertTrue(bbs.get(42));
        assertTrue(bbs.get(24));
        assertFalse(bbs.get(84));

        assertEquals(0, idx.searchIds(22, false, bbs));

    }

}
