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

package io.zorka.tdb.test.bench;

import io.zorka.tdb.test.support.*;

import io.zorka.tdb.text.fm.FmCompressionLevel;
import io.zorka.tdb.text.fm.FmIndexFileStore;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import io.zorka.tdb.text.fm.FmTextIndex;
import io.zorka.tdb.text.WalTextIndex;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import static org.junit.Assert.*;

import static io.zorka.tdb.text.fm.FmCompressionLevel.*;

@Warmup(iterations = 2, time = 4, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 8, time = 2, timeUnit =  TimeUnit.SECONDS)
@Fork(1) @BenchmarkMode(Mode.AverageTime) @OutputTimeUnit(TimeUnit.MICROSECONDS)
public class FmIndexIdSearchBenchmark {

    private final static int[] SIZES = { 1, 2, 4, 8, 16, 32, 64, 128 };

    private final static FmCompressionLevel[] LEVELS = { LEVEL0, LEVEL1, LEVEL2, LEVEL3, LEVEL4 };

    @Test
    public void runBenchmark() throws Exception {
        TestStrGen tsg = new MutatingStrGen(
                new VerifyingStrGen(new JarScanStrGen(), true, false),
                MutatingStrGen.SERIAL, 32);

        File wf = new File("/tmp", "FmIndexBenchmark.wal");
        if (wf.exists()) {
            assertEquals(true, wf.delete());
        }

        WalTextIndex wal = new WalTextIndex("/tmp/FmIndexBenchmark.wal", 0, 512 * ZicoTestFixture.MB);

        for (int sz : SIZES) {
            for (FmCompressionLevel level : LEVELS) {
                File f = new File("/tmp", "FmIndexBenchmark_" + sz + "_" + level + ".ifm");
                if (!f.exists()) {
                    System.out.println("Generating data for: " + f);
                    while (wal.getDatalen() < sz * ZicoTestFixture.MB) {
                        wal.add(tsg.get());
                    }
                    FmIndexFileStoreBuilder fib = new FmIndexFileStoreBuilder(f, level);
                    System.out.println("Compressing: " + f);
                    fib.walToFm(wal);
                }

            }
        }
        wal.close();

        System.setProperty(BenchmarkUtil.STACK_PROFILE, "true");
        BenchmarkUtil.launch(this.getClass().getName() + ".*");
    }


    @State(Scope.Thread)
    public static class BenchmarkState {

        FmIndexFileStore fif;
        FmTextIndex idx;
        Random rnd = new Random();

        public FmTextIndex get(int sz, FmCompressionLevel level) {
            if (idx == null) {
                File f = new File("/tmp", "FmIndexBenchmark_" + sz + "_" + level + ".ifm");
                System.out.println("Opening: " + f);
                fif = new FmIndexFileStore(f.getPath(), FmIndexFileStore.CHECK_ALL_SUMS);
                idx = new FmTextIndex(fif);
            }
            return idx;
        }
    }

    private void benchmarkID(BenchmarkState state, Blackhole hole, int sz, FmCompressionLevel level) {
        Random rnd = state.rnd;
        FmTextIndex idx = state.get(sz, level);
        int len = (int)idx.getNWords();

        for (int i = 0; i < 1000; i++) {
            hole.consume(idx.get(rnd.nextInt(len)));
        }
    }

    @Benchmark
    public void ID_001_LEVEL0(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 1, FmCompressionLevel.LEVEL0);
    }

    @Benchmark
    public void ID_001_LEVEL1(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 1, FmCompressionLevel.LEVEL1);
    }

    @Benchmark
    public void ID_001_LEVEL2(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 1, FmCompressionLevel.LEVEL2);
    }

    @Benchmark
    public void ID_001_LEVEL3(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 1, FmCompressionLevel.LEVEL3);
    }

    @Benchmark
    public void ID_001_LEVEL4(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 1, FmCompressionLevel.LEVEL4);
    }


    // --------------- 4 MB ----------------


    @Benchmark
    public void ID_002_LEVEL0(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 2, FmCompressionLevel.LEVEL0);
    }

    @Benchmark
    public void ID_002_LEVEL1(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 2, FmCompressionLevel.LEVEL1);
    }

    @Benchmark
    public void ID_002_LEVEL2(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 2, FmCompressionLevel.LEVEL2);
    }

    @Benchmark
    public void ID_002_LEVEL3(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 2, FmCompressionLevel.LEVEL3);
    }

    @Benchmark
    public void ID_002_LEVEL4(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 2, FmCompressionLevel.LEVEL4);
    }


    // --------------- 4 MB ----------------


    @Benchmark
    public void ID_004_LEVEL0(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 4, FmCompressionLevel.LEVEL0);
    }

    @Benchmark
    public void ID_004_LEVEL1(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 4, FmCompressionLevel.LEVEL1);
    }

    @Benchmark
    public void ID_004_LEVEL2(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 4, FmCompressionLevel.LEVEL2);
    }

    @Benchmark
    public void ID_004_LEVEL3(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 4, FmCompressionLevel.LEVEL3);
    }

    @Benchmark
    public void ID_004_LEVEL4(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 4, FmCompressionLevel.LEVEL4);
    }


    // --------------- 8 MB ----------------


    @Benchmark
    public void ID_008_LEVEL0(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 8, FmCompressionLevel.LEVEL0);
    }

    @Benchmark
    public void ID_008_LEVEL1(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 8, FmCompressionLevel.LEVEL1);
    }

    @Benchmark
    public void ID_008_LEVEL2(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 8, FmCompressionLevel.LEVEL2);
    }

    @Benchmark
    public void ID_008_LEVEL3(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 8, FmCompressionLevel.LEVEL3);
    }

    @Benchmark
    public void ID_008_LEVEL4(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 8, FmCompressionLevel.LEVEL4);
    }


    // --------------- 16 MB ----------------


    @Benchmark
    public void ID_016_LEVEL0(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 16, FmCompressionLevel.LEVEL0);
    }

    @Benchmark
    public void ID_016_LEVEL1(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 16, FmCompressionLevel.LEVEL1);
    }

    @Benchmark
    public void ID_016_LEVEL2(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 16, FmCompressionLevel.LEVEL2);
    }

    @Benchmark
    public void ID_016_LEVEL3(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 16, FmCompressionLevel.LEVEL3);
    }

    @Benchmark
    public void ID_016_LEVEL4(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 16, FmCompressionLevel.LEVEL4);
    }


    // --------------- 32 MB ----------------


    @Benchmark
    public void ID_032_LEVEL0(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 32, FmCompressionLevel.LEVEL0);
    }

    @Benchmark
    public void ID_032_LEVEL1(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 32, FmCompressionLevel.LEVEL1);
    }

    @Benchmark
    public void ID_032_LEVEL2(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 32, FmCompressionLevel.LEVEL2);
    }

    @Benchmark
    public void ID_032_LEVEL3(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 32, FmCompressionLevel.LEVEL3);
    }

    @Benchmark
    public void ID_032_LEVEL4(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 32, FmCompressionLevel.LEVEL4);
    }


    // --------------- 64 MB ----------------


    @Benchmark
    public void ID_064_LEVEL0(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 64, FmCompressionLevel.LEVEL0);
    }

    @Benchmark
    public void ID_064_LEVEL1(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 64, FmCompressionLevel.LEVEL1);
    }

    @Benchmark
    public void ID_064_LEVEL2(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 64, FmCompressionLevel.LEVEL2);
    }

    @Benchmark
    public void ID_064_LEVEL3(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 64, FmCompressionLevel.LEVEL3);
    }

    @Benchmark
    public void ID_064_LEVEL4(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 64, FmCompressionLevel.LEVEL4);
    }


    // --------------- 128 MB ----------------


    @Benchmark
    public void ID_128_LEVEL0(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 128, FmCompressionLevel.LEVEL0);
    }

    @Benchmark
    public void ID_128_LEVEL1(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 128, FmCompressionLevel.LEVEL1);
    }

    @Benchmark
    public void ID_128_LEVEL2(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 128, FmCompressionLevel.LEVEL2);
    }

    @Benchmark
    public void ID_128_LEVEL3(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 128, FmCompressionLevel.LEVEL3);
    }

    @Benchmark
    public void ID_128_LEVEL4(BenchmarkState state, Blackhole hole) throws Exception {
        benchmarkID(state, hole, 128, FmCompressionLevel.LEVEL4);
    }

}
