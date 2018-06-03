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

package io.zorka.tdb.test.support;

import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class BenchmarkUtil {

    public static final String STACK_PROFILE = "bench.stack.profile";
    public static final String GC_PROFILE = "bench.gc.profile";
    public static final String NO_ONLINE = "bench.no.online";

    public static void launch(String...includes) throws RunnerException {
        ChainedOptionsBuilder builder = new OptionsBuilder()
                //.include(include)
                //.threads(1)
                .shouldFailOnError(true)
                .shouldDoGC(true);
                //.jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintInlining")
                //.addProfiler(SomeProfiler.class)

        for (String include : includes) {
            builder = builder.include(include);
        }

        if ("true".equalsIgnoreCase(System.getProperty(GC_PROFILE, "false"))) {
            builder = builder.addProfiler(GCProfiler.class);
        }

        if ("true".equalsIgnoreCase(System.getProperty(STACK_PROFILE, "false"))) {
            builder = builder.addProfiler(StackProfiler.class);
        }

        if ("true".equalsIgnoreCase(System.getProperty(NO_ONLINE, "false"))) {
            builder = builder.jvmArgsAppend("-XX:MaxInlineSize=0");
        }

        new Runner(builder.build()).run();
    }

}
