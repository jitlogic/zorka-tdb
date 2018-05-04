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

package io.zorka.tdb.test.bench;

import io.zorka.tdb.meta.MetadataQuickIndex;
import io.zorka.tdb.meta.MetadataSearchQuery;
import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.test.support.BenchmarkUtil;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.util.IntegerSeqResult;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

@Warmup(iterations = 2, time = 4, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 8, timeUnit =  TimeUnit.SECONDS)
@Fork(1) @BenchmarkMode(Mode.AverageTime) @OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MetadataQuickIndexBenchmark {

    @Test
    public void fullBenchmark() throws Exception {
        System.setProperty(BenchmarkUtil.STACK_PROFILE, "true");
        System.setProperty(BenchmarkUtil.GC_PROFILE, "true");
        //mSystem.setProperty(BenchmarkUtil.NO_ONLINE, "true");
        BenchmarkUtil.launch(this.getClass().getName() + ".*");
    }

    @State(Scope.Thread)
    public static class BenchmarkState {
        private Map<Integer,MetadataQuickIndex> imap = new HashMap<>();

        public synchronized MetadataQuickIndex get(int nrecs) {

            if (imap.containsKey(nrecs)) {
                return imap.get(nrecs);
            }


            File f = new File("/tmp/qidx_" + nrecs + ".mqi");
            boolean fe = f.exists();

            MetadataQuickIndex idx = new MetadataQuickIndex(f);
            imap.put(nrecs, idx);

            if (!fe) {
                for (int i = 0; i < nrecs; i++) {
                    idx.add(ZicoTestFixture.md(i+1, (i%4)+1, (i%8)+1, (i%4)+1,
                        (i+10) * 1000, 10 + 10 * (i % 500), 0 == i % 20,
                        i * 200 + 1, 0));
                }
            }

            return imap.get(nrecs);
        }
    }

    @Benchmark @Threads(1)
    public void QRY_1024(BenchmarkState state, Blackhole hole) {
        benchmarkQueryGeneric(state, hole, 1024);
    }

    @Benchmark @Threads(1)
    public void QRY_32768(BenchmarkState state, Blackhole hole) {
        benchmarkQueryGeneric(state, hole, 32768);
    }


    public void benchmarkQueryGeneric(BenchmarkState state, Blackhole hole, int nrecs) {
        MetadataQuickIndex mqi = state.get(nrecs);
        ChunkMetadata tm = new ChunkMetadata();
        MetadataSearchQuery mq = new MetadataSearchQuery();
        mq.setTstop(Long.MAX_VALUE);
        int pos = 0;
        for (int i = 0; i < 1000; i++) {
            //tm.setAppId(i % 8 + 1);
            //tm.setAppId(0);
            //tm.setErrorFlag(true);
            IntegerSeqResult rslt = mqi.search(mq); // TODO this is not proper benchmark
            //pos = rslt.getLastPos();
            hole.consume(pos);
        }
    }
}