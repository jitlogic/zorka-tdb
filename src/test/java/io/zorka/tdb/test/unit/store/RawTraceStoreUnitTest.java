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

package io.zorka.tdb.test.unit.store;

import io.zorka.tdb.store.RawTraceDataFile;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.util.CborDataReader;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;

/**
 *
 */
public class RawTraceStoreUnitTest extends ZicoTestFixture {


    @Test
    public void testStoreAndReadZLib() throws Exception {
        RawTraceDataFile tdf = new RawTraceDataFile(new File(tmpDir, "traces.dat"), true);

        long pos1 = tdf.write("ABCDE".getBytes());
        long pos2 = tdf.write("GHIJ".getBytes());

        CborDataReader rdr1 = tdf.read(pos1);
        assertEquals(5, rdr1.size());
        assertEquals(65, rdr1.peek());

        CborDataReader rdr2 = tdf.read(pos2);
        assertEquals(4, rdr2.size());
        assertEquals(71, rdr2.peek());
    }


    @Test
    public void testStoreAndReadLZ4() throws Exception {
        RawTraceDataFile tdf = new RawTraceDataFile(new File(tmpDir, "traces.dat"),
            true, RawTraceDataFile.LZ4_COMPRESSION);

        long pos1 = tdf.write("ABCDE".getBytes());
        long pos2 = tdf.write("GHIJ".getBytes());

        CborDataReader rdr1 = tdf.read(pos1);
        assertEquals(5, rdr1.size());
        assertEquals(65, rdr1.peek());

        CborDataReader rdr2 = tdf.read(pos2);
        assertEquals(4, rdr2.size());
        assertEquals(71, rdr2.peek());
    }


    @Test
    public void testStoreAndReadRaw() throws Exception {
        RawTraceDataFile tdf = new RawTraceDataFile(new File(tmpDir, "traces.dat"),
            true, RawTraceDataFile.NO_COMPRESSION);

        long pos1 = tdf.write("ABCDE".getBytes());
        long pos2 = tdf.write("GHIJ".getBytes());

        CborDataReader rdr1 = tdf.read(pos1);
        assertEquals(5, rdr1.size());
        assertEquals(65, rdr1.peek());

        CborDataReader rdr2 = tdf.read(pos2);
        assertEquals(4, rdr2.size());
        assertEquals(71, rdr2.peek());
    }
}
