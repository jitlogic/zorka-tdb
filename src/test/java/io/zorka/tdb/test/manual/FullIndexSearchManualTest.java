/*
 * Copyright (c) 2012-2017 Rafał Lewczuk All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zorka.tdb.test.manual;

import io.zorka.tdb.store.*;
import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.store.RotatingTraceStore;
import io.zorka.tdb.store.StoreSearchQuery;
import io.zorka.tdb.store.TraceDataIndexer;
import io.zorka.tdb.store.TraceSearchResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertTrue;

public class FullIndexSearchManualTest {

    protected Map<String,TraceDataIndexer> indexerCache = new ConcurrentHashMap<>();

    RotatingTraceStore store = null;

    public void openStore(String path) throws Exception {
        File baseDir = new File(path);
        assertTrue("Store directory not found", baseDir.exists());
        store = new RotatingTraceStore(baseDir, new Properties(), s -> 1, Runnable::run, Runnable::run, indexerCache);
    }

    @After
    public void closeStore() throws Exception {
        if (store != null) {
            store.close();
            store = null;
        }
    }

    @Test
    public void testSearchSomethingInBigStore() throws Exception {
        openStore("/v/zorka/testtrdb/vol1");
        StoreSearchQuery query = new StoreSearchQuery();
        //query.addPattern(SearchPattern.search("/king//index.xhtml"));
        //query.addPattern(SearchPattern.search("lmthx.xedni//gnik/"));
        //query.addPattern(SearchPattern.search("lmthx"));
        query.addPattern("xhtml");
        //SimpleTraceStore.SEARCH_QR_THRESHOLD = 32;
        TraceSearchResult result = store.search(query);
        long rslt = result.getNext();
        int nResults = 0;
        while (rslt > 0) {
            System.out.println("Result: " + rslt);
            nResults++;
            rslt = result.getNext();
        }
        System.out.println("NRESULTS=" + nResults);
        assertTrue("There should be at least some results.", nResults > 0);
    }

}
