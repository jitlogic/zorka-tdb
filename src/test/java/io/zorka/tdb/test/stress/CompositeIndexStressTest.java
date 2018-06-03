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

package io.zorka.tdb.test.stress;

import io.zorka.tdb.test.support.ZicoTestFixture;

import java.io.*;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.*;

import io.zorka.tdb.text.ci.CompositeIndexStore;
import io.zorka.tdb.text.ci.CompositeIndex;
import io.zorka.tdb.text.ci.CompositeIndexFileStore;
import io.zorka.tdb.util.ZicoUtil;
import io.zorka.tdb.text.ci.CompositeIndexStore;
import org.junit.Ignore;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;

import static org.junit.Assume.*;

public class CompositeIndexStressTest extends ZicoTestFixture {

    @Test @Ignore("Fix me.")
    public void testPopulateCompositeIndex() throws Exception {
//        CompositeIndex idx = new CompositeIndex(new File(tmpDir), "test");
//        TestStrGen gen = getSerialStrGen();
//
//        for (int i = 0; i < 16 * 1024 * 1024; i++) {
//            idx.add(gen.get());
//        }
//
//        Thread.sleep(5000);
//
//        idx.close();

    }

    private static Executor executor = Executors.newSingleThreadExecutor();
    private CompositeIndexStore fbs;
    private CompositeIndex cti;

    private long t1 = System.currentTimeMillis(), ts = 0;
    private long r1 = 0, r2 = 0;   // Number of records added
    private long s1 = 0, s2 = 0;
    //private long a0 = 0, a1 = 0;   // Add operations


    private void runPackAndMeasure(Runnable task) {
        long t2 = System.currentTimeMillis(), t = t2 - t1;
        long r = r2 - r1, s = s2 - s1;

        ts += t;

        task.run();

        long t3 = System.currentTimeMillis();

        System.out.println("CHK: r=" + r + " s=" + s + " t=" + t
                + " dr=" + (1000L * r / t)
                + " ds=" + (1000L * s / t)
                + " pt=" + (t3-t2));

        t1 = t3;
        r1 = r2;
        s1 = s2;
    }

    private void runAddAndMeasure(String s) {
        if (s.length() == 0) return;
        try {
            cti.add(s);
            r2++;
            s2 += s.length();
        } catch (Exception e) {
            System.out.println("S = '" + DatatypeConverter.printBase64Binary(s.getBytes()) + "'");
            System.err.println("Error: " + e);
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testPopulateCompositeIndexFromExtractedZicoData() throws Exception {
        int START = 8, STOP = 9;

        for (int i = START; i < STOP; i++) {
            File f = new File(TESTDATA_DIR, "root" + i + ".txt");
            assumeTrue("File " + f + " does not exist.", f.canRead());
        }

        Properties props = ZicoUtil.props("rotation.coalescing_merge", "true", "rotation.base_size", "4096");
        fbs = new CompositeIndexFileStore(tmpDir, "sss", props);
        cti = new CompositeIndex(fbs, props, executor);

        for (int i = START; i < STOP; i++) {
            File f = new File(TESTDATA_DIR, "root" + i + ".txt");
            if (f.exists()) {
                Files.lines(f.toPath()).forEach(this::runAddAndMeasure);
            }
        }

        ts += System.currentTimeMillis() - t1;

        long x = (r2 * 1000) / ts;

        System.out.println("TS=" + ts + ", r2=" + r2 + ", x=" + x);

        // cti.archive() here
        for (int i = 0; i < 12; i++) {
            //Thread.sleep(10000);
            cti.runMaintenance();
            Thread.sleep(10000);
        }
        //Thread.sleep(120 * 1000);
        //cti.archive();
    }
}
