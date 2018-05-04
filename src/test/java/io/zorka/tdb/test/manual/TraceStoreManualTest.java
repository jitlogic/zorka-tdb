package io.zorka.tdb.test.manual;

import io.zorka.tdb.store.SimpleTraceStore;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;

public class TraceStoreManualTest {

    @Test
    public void testOpenBorkenStore() throws Exception {
        SimpleTraceStore store = new SimpleTraceStore(
            new File("/works/zico-collector/data/traces/000000"),
            null, Runnable::run, Runnable::run,
            new HashMap<>(), s->0);
        store.open();
    }

}
