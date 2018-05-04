package io.zorka.tdb.test.unit.text.ci;

import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.text.TextIndexState;
import io.zorka.tdb.text.ci.CompositeIndexState;
import io.zorka.tdb.text.ci.CompositeIndexStore;
import io.zorka.tdb.text.ci.CompositeIndex;
import io.zorka.tdb.text.ci.CompositeIndexFileStore;
import io.zorka.tdb.util.ZicoUtil;

import io.zorka.tdb.text.TextIndexState;
import io.zorka.tdb.text.ci.CompositeIndexStore;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class CompositeIndexFullUnitTest extends ZicoTestFixture {

    @Test
    public void testRunSimpleScenario() {
        Properties props = ZicoUtil.props();
        CompositeIndexStore store = new CompositeIndexFileStore(tmpDir, "test", props);
        Executor executor = Runnable::run;
        CompositeIndex ci = new CompositeIndex(store, props, executor);
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
        CompositeIndex ci = new CompositeIndex(store, props, executor);

        int idA = ci.add("A"), idB = ci.add("B");

        ci.archive();
        ci.runMaintenance();

        CompositeIndexState cs = ci.getCState();
        assertNull(cs.getCurrentIndex());

        assertEquals(1, cs.getLookupIndexes().size());
        assertTrue("Expected FM index in lookup list.", cs.getLookupIndexes().get(0).isReadOnly());
        assertEquals(TextIndexState.REMOVAL, cs.getAllIndexes().get(0).getState());
        assertEquals(1, cs.getSearchIndexes().size());
        assertTrue(cs.getSearchIndexes().get(0).isReadOnly());
        assertEquals(2, cs.getAllIndexes().size());

        assertNotEquals(-1, idA);
        assertEquals(idA, ci.get("A"));
        assertNotEquals(-1, idB);
        assertEquals(idB, ci.get("B"));
    }

}
