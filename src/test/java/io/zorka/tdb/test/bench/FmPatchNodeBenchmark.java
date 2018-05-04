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
import io.zorka.tdb.text.fm.FmPatchNode;
import io.zorka.tdb.text.fm.FmPatchNodeL;
import io.zorka.tdb.text.fm.FmPatchNode;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 8, time = 2, timeUnit =  TimeUnit.SECONDS)
@Fork(1) @BenchmarkMode(Mode.AverageTime) @OutputTimeUnit(TimeUnit.MICROSECONDS)
public class FmPatchNodeBenchmark {

    public final static int LENGTH = Integer.MAX_VALUE - 32 * 1024 * 1024;
    public final static int INITSZ = 16 * 1024 * 1024;


    @Test
    public void fullBenchmark() throws Exception {
        //System.setProperty(BenchmarkUtil.STACK_PROFILE, "true");
        System.setProperty(BenchmarkUtil.GC_PROFILE, "true");
        //mSystem.setProperty(BenchmarkUtil.NO_ONLINE, "true");
        BenchmarkUtil.launch(this.getClass().getName() + ".*repack");
    }


    @State(Scope.Thread)
    public static class BenchmarkState {
        Random rnd = new Random();

        FmPatchNode node = new FmPatchNodeL(LENGTH);

        @Setup(Level.Trial)
        public void initialize() {
            System.out.println("Initialization started.");
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < INITSZ; i++) {
                node = node.insert(rnd.nextInt(LENGTH+i), (byte)(rnd.nextInt()));
            }
            long t2 = System.currentTimeMillis();
            System.out.println("Initialization finished (t=" + (t2-t1) + ")");
        }
    }

    @Benchmark
    public void fm_ins(BenchmarkState state, Blackhole hole) {
        FmPatchNode node = state.node;
        for (int i = 0; i < 1000; i++) {
            node = node.insert(state.rnd.nextInt(LENGTH), (byte)(state.rnd.nextInt()));
        }
    }

    @Benchmark
    public void fm_size(BenchmarkState state, Blackhole hole) {
        FmPatchNode node = state.node;
        Random rnd = state.rnd;

        for (int i = 0; i < 1000; i++) {
            hole.consume(node.size(rnd.nextInt(LENGTH)));
        }
    }

    @Benchmark
    public void fm_rank(BenchmarkState state, Blackhole hole) {
        FmPatchNode node = state.node;
        Random rnd = state.rnd;

        for (int i = 0; i < 1000; i++) {
            hole.consume(node.rankOf(rnd.nextInt(LENGTH), (byte)(rnd.nextInt())));
        }
    }

    @Benchmark
    public void fm_repack(BenchmarkState state, Blackhole hole) {
        FmPatchNode node = state.node;
        Random rnd = state.rnd;
        int l = node.length();
        for (int i = 0; i < 1000; i++) {
            int pos = rnd.nextInt(l+i);
            byte ch = (byte) rnd.nextInt();
            hole.consume(node.rankOf(pos, ch)); // rankOf
            hole.consume(node.size(pos));
            hole.consume(node.getCharOffs(ch));
            node = node.insert(pos, ch);
        }
    }

}
