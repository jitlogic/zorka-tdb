package io.zorka.tdb.test.unit.text;

import io.zorka.tdb.text.TextIndexType;
import io.zorka.tdb.text.WritableTextIndex;
import io.zorka.tdb.search.ssn.TextNode;
import io.zorka.tdb.search.tsn.KeyValSearchNode;
import io.zorka.tdb.util.BitmapSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TextSearchUnitTest extends TextIndexTestFixture {

    private int[] ids;

    public TextSearchUnitTest(TextIndexType type) {
        super(type);
    }

    private void loadData(WritableTextIndex idx) {
        String[] str = new String[] { "AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH", "III", "JJJ" };
        ids = new int[str.length];
        for (int i = 0; i < str.length; i++) {
            ids[i] = idx.add(str[i].getBytes());
            assertNotEquals(-1, ids[i]);
        }
    }

    private void loadData1(WritableTextIndex idx) {
        ids = new int[101];
        for (int i = 0; i < 100;  i++) {
            ids[i] = idx.add("blop"+i);
            assertNotEquals(-1, ids[i]);
        }
        ids[100] = idx.add("XYZ");
    }

    @Test
    public void testNonTerminatedSearch() throws Exception {
        newIndex(this::loadData);
        BitmapSet bbs = new BitmapSet();
        int cnt = idx.search(new TextNode("C", false, false), bbs);

        assertEquals(1, cnt);
        assertTrue(bbs.get(ids[2]));
    }


    @Test
    public void testSearchNonExistentItem() throws Exception {
        newIndex(this::loadData);
        BitmapSet bbs = new BitmapSet();
        assertEquals(0, idx.search(new TextNode("X", false, false), bbs));
    }


    @Test
    public void testSearchInvalidType() throws Exception {
        newIndex(this::loadData);
        BitmapSet bbs = new BitmapSet();
        assertEquals(0, idx.search(new KeyValSearchNode("a", new TextNode("ss", false, false)), bbs));
    }


    @Test
    public void testSearchWithLongIds() throws Exception {
        newIndex(this::loadData1);
        BitmapSet bbs = new BitmapSet();
        int cnt = idx.search(new TextNode("XYZ", false, false), bbs);
        assertEquals(1, cnt);
        assertTrue(bbs.get(ids[100]));
    }
}
