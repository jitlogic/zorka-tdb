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

package io.zorka.tdb.test.stress;

import io.zorka.tdb.text.fm.LZ4Extractor;
import io.zorka.tdb.util.lz4.LZ4HCJavaSafeCompressor;

import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.util.lz4.LZ4JavaSafeDecompressor;

import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.util.lz4.LZ4JavaSafeDecompressor;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

import static io.zorka.tdb.text.fm.LZ4Extractor.chr;
import static io.zorka.tdb.text.fm.LZ4Extractor.rank;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 */
public class LZ4DecompressorStressTest {

    private final static int BSIZE = 65536;

    @Test
    public void testSafeCompressAndSafeDecompress() throws Exception {
        byte[] idata = TestUtil.readf("LICENSE.txt", BSIZE, false);
        byte[] cdata = new byte[BSIZE], odata = new byte[BSIZE];

        int clen = LZ4HCJavaSafeCompressor.INSTANCE.compress(idata, 0, idata.length, cdata, 0, 65536);

        int olen = LZ4JavaSafeDecompressor.INSTANCE.decompress(cdata, 0, odata, 0, idata.length);

        if (clen != olen) {
            fail("Decompressor should consume all compressed input.");
        }

        TestUtil.arraysEqual("Original <-> uncompressed.", idata, odata, idata.length);
    }

    @Test
    public void testCompressAndExtractCharAndRankUnsafeWithoutReset() throws Exception {
        byte[] idata = TestUtil.readf("testdata/bwt01_64k.dat", BSIZE, false);
        byte[] cdata = new byte[BSIZE];
        int[]  ranks = new int[256];

        int clen = LZ4HCJavaSafeCompressor.INSTANCE.compress(idata, 0, idata.length, cdata, 0, 65536);

        ByteBuffer cbuf = ByteBuffer.allocateDirect(BSIZE);
        cbuf.put(cdata);

        long caddr = ((DirectBuffer)cbuf).address();

        LZ4Extractor extr = new LZ4Extractor();

        for (int i = 0; i < idata.length; i++) {
            int x = extr.charAndRank(caddr, clen, i);
            byte c1 = chr(x), c2 = idata[i];
            int r1 = rank(x), r2 = ranks[x & 0xff];

            assertEquals("Character mismatch at pos " + i, c2, c1);
            assertEquals("Character mismatch at pos " + i, r2, r1);

            ranks[x & 0xff]++;
        }
    }

    @Test
    public void testCompressAndExtractCharAndRankUnsafeWithReset() throws Exception {
        byte[] idata = TestUtil.readf("testdata/bwt01_64k.dat", BSIZE, false);
        byte[] cdata = new byte[BSIZE];
        int[]  ranks = new int[256];

        int clen = LZ4HCJavaSafeCompressor.INSTANCE.compress(idata, 0, idata.length, cdata, 0, 65536);

        ByteBuffer cbuf = ByteBuffer.allocateDirect(BSIZE);
        cbuf.put(cdata);

        long caddr = ((DirectBuffer)cbuf).address();

        LZ4Extractor extr = new LZ4Extractor();

        for (int i = 0; i < idata.length; i++) {
            extr.reset();
            int x = extr.charAndRank(caddr, clen, i);
            byte c1 = chr(x), c2 = idata[i];
            int r1 = rank(x), r2 = ranks[x & 0xff];

            assertEquals("Character mismatch at pos " + i, c2, c1);
            assertEquals("Character mismatch at pos " + i, r2, r1);

            ranks[x & 0xff]++;
        }
    }

}
