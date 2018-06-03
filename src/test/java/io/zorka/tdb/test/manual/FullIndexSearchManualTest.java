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

package io.zorka.tdb.test.manual;

import io.zorka.tdb.store.RotatingTraceStore;
import io.zorka.tdb.store.TraceDataIndexer;
import org.junit.After;
import org.junit.Ignore;
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

    @Test @Ignore("TODO przepisać z nowym API")
    public void testSearchSomethingInBigStore() throws Exception {
        openStore("/v/zorka/testtrdb/vol1");
//        StoreSearchQuery query = new StoreSearchQuery();
        //query.addPattern(SearchPattern.search("/king//index.xhtml"));
        //query.addPattern(SearchPattern.search("lmthx.xedni//gnik/"));
        //query.addPattern(SearchPattern.search("lmthx"));
//        query.addPattern("xhtml");
        //SimpleTraceStore.SEARCH_QR_THRESHOLD = 32;
//        TraceSearchResult result = store.search(query);
//        long rslt = result.getNext();
//        int nResults = 0;
//        while (rslt > 0) {
//            System.out.println("Result: " + rslt);
//            nResults++;
//            rslt = result.getNext();
//        }
//        System.out.println("NRESULTS=" + nResults);
//        assertTrue("There should be at least some results.", nResults > 0);
    }

}
