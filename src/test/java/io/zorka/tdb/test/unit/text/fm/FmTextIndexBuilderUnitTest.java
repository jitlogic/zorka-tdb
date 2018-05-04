/*
 * Copyright 2016-2017 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
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

package io.zorka.tdb.test.unit.text.fm;

import io.zorka.tdb.text.fm.FmBlockDesc;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.zorka.tdb.text.fm.FmBlockDesc;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import org.junit.Test;
import static org.junit.Assert.*;

public class FmTextIndexBuilderUnitTest {

    private static FmIndexFileStoreBuilder indexer;

    static {
        try {
            indexer  = new FmIndexFileStoreBuilder(new File("/tmp/test.ifm"), 4, 4, 80, 1, 32);
        } catch (IOException e) {
            System.out.println("Cannot create indexer.");
        }
    }

    private List<FmBlockDesc> bds(int...l) {
        List<FmBlockDesc> lst = new ArrayList<>();
        for (int i = 2; i < l.length; i+=3) {
            lst.add(new FmBlockDesc(l[i-2],l[i-1],l[i]));
        }
        return lst;
    }


    private void checkGaps(String s, int...l) {
        List<FmBlockDesc> lst = bds(l);
        byte[] data = s.getBytes();
        assertEquals( //"Data: " + s,
            lst, indexer.findGaps(data, 0, data.length));
    }


    @Test
    public void testFindGaps() {
        checkGaps("abbccdde");
        checkGaps("aaaabcde", 0,4,0);
        checkGaps("abbbbcde", 1,4,0);
        checkGaps("abbbbbbcde", 1,6,0);
        checkGaps("aaaabbbb", 0,4,0,  4,4,0);
        checkGaps("aaaabbbbbbcccdddde", 0,4,0,  4,6,0,  13,4,0);
    }

    @Test
    public void testFindBlocks() {
        assertEquals( //"One gap, no splits.",
                bds(0,2,1,  2,4,0,  6,2,1),
                indexer.findBlocks(bds(2,4,0), 0, 8));
        assertEquals( //"One gap, splits.",
                bds(0,4,1,  4,2,1,  6,4,0,  10,4,1,  14,2,1),
                indexer.findBlocks(bds(6,4,0), 0, 16));
        assertEquals( //"Two gaps, splits.",
                bds(0,4,1,  4,2,1,  6,4,0,  10,4,1,  14,2,1,  16,4,0,  20,4,1,  24,4,1),
                indexer.findBlocks(bds(6,4,0,  16,4,0), 0, 28));
    }

}
