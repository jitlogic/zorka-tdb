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

import io.zorka.tdb.test.support.BenchmarkUtil;
import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.text.fm.LZ4Extractor;
import io.zorka.tdb.util.lz4.LZ4HCJavaSafeCompressor;
import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.text.fm.LZ4Extractor;
import io.zorka.tdb.util.lz4.LZ4HCJavaSafeCompressor;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 8, time = 1, timeUnit =  TimeUnit.SECONDS)
@Fork(1) @BenchmarkMode(Mode.AverageTime) @OutputTimeUnit(TimeUnit.MICROSECONDS)
public class LZ4ExtractorBenchmark {


    @Test
    public void fullBenchmark() throws Exception {
        BenchmarkUtil.launch(this.getClass().getName() + ".*");
    }


    @Test
    public void xt_LZ4_A_01024__Benchmark() throws Exception {
        BenchmarkUtil.launch(this.getClass().getName() + ".xt_LZ4_A_01024");
    }


    @State(Scope.Thread)
    public static class BenchmarkState {
        byte[] idata, cdata;
        int ilen, clen;

        ByteBuffer cbuf;
        long caddr;

        @Setup(Level.Trial)
        public void initialize() {
            idata = TestUtil.readf("testdata/bwt01_64k.dat", 65536, false);
            ilen = idata.length;
            cdata = new byte[ilen];
            clen = LZ4HCJavaSafeCompressor.INSTANCE.compress(idata, 0, idata.length, cdata, 0, cdata.length);

            cbuf = ByteBuffer.allocateDirect(ilen);
            cbuf.put(cdata);

            caddr = ((DirectBuffer)cbuf).address();
        }
    }

    private void xt_LZ4_D(BenchmarkState state, Blackhole hole, int sz) {
        int step = sz >>> 10;
        LZ4Extractor xtr = new LZ4Extractor();
        for (int i = 0; i < 1024; i++) {
            hole.consume(xtr.charAndRank(state.caddr, state.clen, i * step));
        }
    }

    @Benchmark
    public void xt_LZ4_D_01024(BenchmarkState state, Blackhole hole) {
        xt_LZ4_D(state, hole, 1024);
    }

    @Benchmark
    public void xt_LZ4_D_02048(BenchmarkState state, Blackhole hole) {
        xt_LZ4_D(state, hole, 2048);
    }

    @Benchmark
    public void xt_LZ4_D_04096(BenchmarkState state, Blackhole hole) {
        xt_LZ4_D(state, hole, 4096);
    }

    @Benchmark
    public void xt_LZ4_D_08192(BenchmarkState state, Blackhole hole) {
        xt_LZ4_D(state, hole, 8192);
    }

    @Benchmark
    public void xt_LZ4_D_16384(BenchmarkState state, Blackhole hole) {
        xt_LZ4_D(state, hole, 16384);
    }

    @Benchmark
    public void xt_LZ4_D_32768(BenchmarkState state, Blackhole hole) {
        xt_LZ4_D(state, hole, 32768);
    }

    @Benchmark
    public void xt_LZ4_D_65536(BenchmarkState state, Blackhole hole) {
        xt_LZ4_D(state, hole, 65536);
    }

}
