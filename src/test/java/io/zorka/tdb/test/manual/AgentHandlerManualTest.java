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

package io.zorka.tdb.test.manual;

import io.zorka.tdb.store.ConfigProps;
import io.zorka.tdb.store.RotatingTraceStore;
import io.zorka.tdb.store.SimpleTraceStore;
import io.zorka.tdb.store.TraceStore;
import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.util.ZicoUtil;

import java.io.*;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.zip.GZIPInputStream;

import io.zorka.tdb.store.RotatingTraceStore;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

/**
 *
 */

public class AgentHandlerManualTest extends ZicoTestFixture {

    public static class InstrumentedExecutor implements Executor {

        public long t = 0;

        @Override
        public void execute(Runnable command) {
            long t1 = System.currentTimeMillis();
            command.run();
            long t2 = System.currentTimeMillis();
            t += (t2-t1);
        }
    }

    private InstrumentedExecutor ex;
    private TraceStore store;

    @Before
    public void initStore() throws Exception {
        File spath = new File("/tmp/tstore");
        if (spath.exists()) {
            TestUtil.rmrf(spath);
        }
        spath.mkdirs();

        Properties props = ZicoUtil.props(
            ConfigProps.STORES_MAX_NUM, "8",
            ConfigProps.STORES_MAX_SIZE, "512",
            ConfigProps.MIDX_WAL_SIZE, "256",
            ConfigProps.MIDX_WAL_NUM, "8");

        ex = new InstrumentedExecutor();
        store = new RotatingTraceStore(spath, props, s->0, ex, ex, new ConcurrentHashMap<>());
    }


    @Test
    public void testLoadData() throws Exception {
        LineNumberReader rdr = new LineNumberReader(new BufferedReader(
            new InputStreamReader(new GZIPInputStream(new FileInputStream(
                "/v/traces/vol/_output/sar.pjapp1_4.ztrc"))), 1024 * 1024));
        String line;
        int cnt = 0, nagd = 0, ntrc = 0;

        long t1 = System.currentTimeMillis();

        while (null != (line = rdr.readLine())) {
            String uuid = line.substring(0, line.indexOf(32));
            String uri = line.substring(uuid.length()+1, line.indexOf(32, uuid.length()+1));
            String content = line.substring(uuid.length()+uri.length()+2, line.length());

            switch (uri) {
                case "/agent":
                    System.out.print("@");
                    nagd++;
                    store.handleAgentData(uuid, store.getSession(uuid), content);
                    break;
                case "/trace":
                    System.out.print(".");
                    ntrc++;
                    store.handleTraceData(uuid, store.getSession(uuid), UUID.randomUUID().toString(), content, md(1, 2));
                    break;
                default:
                    System.out.println("Illegal URI: " + uri);
            }

            cnt++;

            if (cnt % 80 == 0) {
                System.out.println(" ");
            }
        } // while ()

        long t2 = System.currentTimeMillis();
        long tt = t2 - t1 - ex.t;

        System.out.println();
        System.out.println("T[trc]=" + tt + ", T[ex]=" + ex.t);
        System.out.println("N[trc]=" + ntrc + ", N[agd]=" + nagd);

        store.archive();

    }

}
