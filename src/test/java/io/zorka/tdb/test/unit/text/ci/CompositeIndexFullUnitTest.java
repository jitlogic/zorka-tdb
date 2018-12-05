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

package io.zorka.tdb.test.unit.text.ci;

import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.TextIndexState;
import io.zorka.tdb.text.ci.CompositeIndexState;
import io.zorka.tdb.text.ci.CompositeIndexStore;
import io.zorka.tdb.text.ci.CompositeIndex;
import io.zorka.tdb.text.ci.CompositeIndexFileStore;
import io.zorka.tdb.util.ZicoUtil;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Properties;
import java.util.concurrent.Executor;

public class CompositeIndexFullUnitTest extends ZicoTestFixture {

    @Test
    public void testRunSimpleScenario() {
        Properties props = ZicoUtil.props();
        CompositeIndexStore store = new CompositeIndexFileStore(tmpDir, "test", props);
        Executor executor = Runnable::run;
        CompositeIndex ci = new CompositeIndex(store, props);
        int idA = ci.add("A"), idB = ci.add("B");
        assertNotEquals(-1, idA);
        assertEquals(idA, ci.get("A"));
        assertNotEquals(-1, idB);
        assertEquals(idB, ci.get("B"));
    }


    @Test
    public void testCreatePopulateArchiveIndex() {
        Properties props = ZicoUtil.props();
        CompositeIndexStore store = new CompositeIndexFileStore(tmpDir, "test", props);
        Executor executor = Runnable::run;
        CompositeIndex ci = new CompositeIndex(store, props);

        int idA = ci.add("A"), idB = ci.add("B");

        ci.archive();
        ci.runMaintenance();

        CompositeIndexState cs = ci.getCState();
        assertNull(cs.getCurrentIndex());

        assertEquals("Expected only one index file.",
                1, cs.getLookupIndexes().size());
        assertTrue("Expected FM index in lookup list.",
                cs.getLookupIndexes().get(0).isReadOnly());
        assertEquals("FM index should be open.",
                TextIndexState.OPEN, cs.getAllIndexes().get(0).getState());
        assertEquals(1, cs.getSearchIndexes().size());
        assertTrue(cs.getSearchIndexes().get(0).isReadOnly());
        assertEquals(1, cs.getAllIndexes().size());

        assertNotEquals(-1, idA);
        assertEquals(idA, ci.get("A"));
        assertNotEquals(-1, idB);
        assertEquals(idB, ci.get("B"));
    }

}
