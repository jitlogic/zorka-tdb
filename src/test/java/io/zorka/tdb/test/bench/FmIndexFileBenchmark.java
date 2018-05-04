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

import io.zorka.tdb.test.support.BenchmarkUtil;
import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.fm.FmCompressionLevel;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import io.zorka.tdb.text.fm.FmIndexFileStore;

import org.junit.Test;

import static org.junit.Assume.*;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.util.Random;
import java.util.concurrent.TimeUnit;



import static io.zorka.tdb.text.fm.FmCompressionLevel.*;

@Warmup(iterations = 2, time = 4, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 8, timeUnit =  TimeUnit.SECONDS)
@Fork(1) @BenchmarkMode(Mode.AverageTime) @OutputTimeUnit(TimeUnit.MICROSECONDS)
public class FmIndexFileBenchmark {

    @Test
    public void fullBenchmark() throws Exception {
        // TODO make this universal
        assumeTrue(ZicoTestFixture.T300M_BWT.canRead());
        byte[] bwt = TestUtil.readf(ZicoTestFixture.T300M_BWT.getPath(), 512 * 1024 * 1024, true);
        for (FmCompressionLevel level : new FmCompressionLevel[] { LEVEL0, LEVEL1, LEVEL2, LEVEL3, LEVEL4}) {
            File f = new File("/tmp","T300M_" + level + ".ifm");
            System.out.println("Compressing: " + f);
            if (!f.exists()) {
                FmIndexFileStoreBuilder builder = new FmIndexFileStoreBuilder(ZicoTestFixture.T300M_IFM, level);
                builder.bwtToFm(bwt, 100, 200, 0);
            }
            double c = 100.0 * f.length() / bwt.length;
            System.out.println("Compression ratio for " + f + ": " + c);
        }
        System.setProperty(BenchmarkUtil.STACK_PROFILE, "true");
        BenchmarkUtil.launch(this.getClass().getName() + ".*");
    }


    @State(Scope.Thread)
    public static class BenchmarkState {

        FmIndexFileStore fif;
        Random rnd = new Random();

        public FmIndexFileStore get(FmCompressionLevel level) {
            if (fif == null) {
                File f = new File("/tmp","T300M_" + level + ".ifm");
                System.out.println("Opening: " + f);
                fif = new FmIndexFileStore(f.getPath(), FmIndexFileStore.CHECK_ALL_SUMS);
            }
            return fif;
        }
    }


    private void benchmarkR(BenchmarkState state, Blackhole hole, FmCompressionLevel level) {
        Random rnd = state.rnd;
        FmIndexFileStore fif = state.get(level);
        int len = fif.getDatalen();

        for (int i = 0; i < 1000000; i++) {
            hole.consume(state.fif.rankOf(rnd.nextInt(len), (byte)rnd.nextInt(256)));
        }
    }


    private void benchCR(BenchmarkState state, Blackhole hole, FmCompressionLevel level) {
        Random rnd = state.rnd;
        FmIndexFileStore fif = state.get(level);
        int len = fif.getDatalen();

        for (int i = 0; i < 1000000; i++) {
            hole.consume(state.fif.charAndRank(rnd.nextInt(len)));
        }
    }


    @Benchmark @Threads(1)
    public void CR_T300M_LEVEL0(BenchmarkState state, Blackhole hole) {
        benchCR(state, hole, LEVEL0);
    }


    @Benchmark @Threads(1)
    public void CR_T300M_LEVEL1(BenchmarkState state, Blackhole hole) {
        benchCR(state, hole, LEVEL1);
    }


    @Benchmark @Threads(1)
    public void CR_T300M_LEVEL2(BenchmarkState state, Blackhole hole) {
        benchCR(state, hole, LEVEL2);
    }


    @Benchmark @Threads(1)
    public void CR_T300M_LEVEL3(BenchmarkState state, Blackhole hole) {
        benchCR(state, hole, LEVEL3);
    }


    @Benchmark @Threads(1)
    public void CR_T300M_LEVEL4(BenchmarkState state, Blackhole hole) {
        benchCR(state, hole, LEVEL4);
    }


    @Benchmark @Threads(1)
    public void R_T300M_LEVEL0(BenchmarkState state, Blackhole hole) {
        benchmarkR(state, hole, LEVEL0);
    }


    @Benchmark @Threads(1)
    public void R_T300M_LEVEL1(BenchmarkState state, Blackhole hole) {
        benchmarkR(state, hole, LEVEL1);
    }


    @Benchmark @Threads(1)
    public void R_T300M_LEVEL2(BenchmarkState state, Blackhole hole) {
        benchmarkR(state, hole, LEVEL2);
    }


    @Benchmark @Threads(1)
    public void R_T300M_LEVEL3(BenchmarkState state, Blackhole hole) {
        benchmarkR(state, hole, LEVEL3);
    }


    @Benchmark @Threads(1)
    public void R_T300M_LEVEL4(BenchmarkState state, Blackhole hole) {
        benchmarkR(state, hole, LEVEL4);
    }
}
