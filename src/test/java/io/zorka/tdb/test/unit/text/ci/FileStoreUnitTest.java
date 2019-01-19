/*
 * Copyright 2016-2019 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
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

package io.zorka.tdb.test.unit.text.ci;

import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.text.WritableTextIndex;
import io.zorka.tdb.text.ci.CompositeIndexStore;
import io.zorka.tdb.text.ci.CompositeIndexFileStore;
import io.zorka.tdb.util.ZicoUtil;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class FileStoreUnitTest extends ZicoTestFixture {

    @Test
    public void testMergeIndexes() {
        CompositeIndexStore store = new CompositeIndexFileStore(tmpDir, "test", ZicoUtil.props());

        WritableTextIndex wx1 = store.addIndex(1);
        int idA = wx1.add("A"), idB = wx1.add("B");
        TextIndex rx1 = store.compressIndex(wx1);
        assertEquals(idA, rx1.get("A"));
        assertEquals(idB, rx1.get("B"));

        WritableTextIndex wx2 = store.addIndex(rx1.getIdBase()+rx1.getNWords());
        int idC = wx2.add("C"), idD = wx2.add("D");
        TextIndex rx2 = store.compressIndex(wx2);
        assertEquals(idC, rx2.get("C"));
        assertEquals(idD, rx2.get("D"));

        WritableTextIndex wx3 = store.addIndex(rx2.getIdBase() + rx2.getNWords());
        int idE = wx3.add("E"), idF = wx3.add("F");
        TextIndex rx3 = store.compressIndex(wx3);
        assertEquals(idE, rx3.get("E"));
        assertEquals(idF, rx3.get("F"));

        TextIndex rx = store.mergeIndex(Arrays.asList(rx1, rx2, rx3));
        assertEquals(idA, rx.get("A"));
        assertEquals(idB, rx.get("B"));
        assertEquals(idC, rx.get("C"));
        assertEquals(idD, rx.get("D"));
        assertEquals(idE, rx.get("E"));
        assertEquals(idF, rx.get("F"));
    }

    // TODO more tests for corner cases (+ stress test in separate suite)
}
