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

package io.zorka.tdb.test.unit.text;

import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.*;
import io.zorka.tdb.text.ci.CompositeIndex;
import io.zorka.tdb.text.ci.CompositeIndexFileStore;
import io.zorka.tdb.text.ci.CompositeIndexStore;
import io.zorka.tdb.text.fm.FmCompressionLevel;
import io.zorka.tdb.text.fm.FmIndexFileStore;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import io.zorka.tdb.text.fm.FmTextIndex;
import io.zorka.tdb.text.wal.WalTextIndex;
import io.zorka.tdb.util.IntegerSeqResult;
import io.zorka.tdb.util.ZicoUtil;

import java.io.File;

import org.junit.Test;
import static org.junit.Assert.*;

public class SearchXIBUnitTest extends ZicoTestFixture {

    private static byte[] encodeMeta(Object...args) {
        int len = 0, pos = 0;
        for (Object arg : args) {
            if (arg instanceof Byte) {
                len++;
            } else if (arg instanceof Integer || arg instanceof Long) {
                len += RawDictCodec.idLen(((Number) arg).longValue());
            } else {
                fail("Argument of illegal type: " + arg);
            }
        }

        byte[] rslt = new byte[len];

        for (Object arg : args) {
            if (arg instanceof Byte) {
                rslt[pos++] = (byte)arg;
            } else if (arg instanceof Integer || arg instanceof Long) {
                pos += RawDictCodec.idEncode(rslt, pos, ((Number)arg).longValue());
            }
        }

        return rslt;
    }

    private static byte M1 = 4;
    private static byte M2 = 5;
    private static byte M3 = 6;


    @Test
    public void testSearchXIB1Wal() throws Exception {
        WalTextIndex wal = new WalTextIndex(TestUtil.path(tmpDir, "test.wal"), 1);

        wal.add(encodeMeta(M1, 42, M2, 64, M3));
        wal.add(encodeMeta(M1, 3434, M2, 45, M3));
        wal.add(encodeMeta(M1, 24, M2, 64, M3));
        wal.add(encodeMeta(69, M2, 66, M3));

        assertEquals(ZicoUtil.set(3434), wal.searchXIB(encodeMeta(M2, 45, M3), M1).toSet());
        assertEquals(ZicoUtil.set(24, 42), wal.searchXIB(encodeMeta(M2, 64, M3), M1).toSet());
        assertEquals(ZicoUtil.set(69), wal.searchXIB(encodeMeta(M2, 66, M3), M1).toSet());

        IntegerSeqResult tsi = wal.searchXIB(encodeMeta(M2, 64, M3), M1);
        assertEquals(2, tsi.estimateSize(64));
        assertEquals(ZicoUtil.set(24, 42), tsi.toSet());

        wal.close();
    }

    @Test
    public void testSearchXIB1Fm() throws Exception {
        WalTextIndex wal = new WalTextIndex(TestUtil.path(tmpDir, "test.wal"), 1);

        wal.add(encodeMeta(M1, 42, M2, 64, M3));
        wal.add(encodeMeta(M1, 3434, M2, 45, M3));
        wal.add(encodeMeta(M1, 24, M2, 64, M3));
        wal.add(encodeMeta(69, M2, 66, M3));

        String path = TestUtil.path(tmpDir, "test.ifm");
        FmIndexFileStoreBuilder builder = new FmIndexFileStoreBuilder(new File(path), FmCompressionLevel.LEVEL2);
        builder.walToFm(wal);

        wal.close();

        FmTextIndex ifm = new FmTextIndex(new FmIndexFileStore(path, 0));

        assertEquals(ZicoUtil.set(3434), ifm.searchXIB(encodeMeta(M2, 45, M3), M1).toSet());
        assertEquals(ZicoUtil.set(24, 42), ifm.searchXIB(encodeMeta(M2, 64, M3), M1).toSet());
        assertEquals(ZicoUtil.set(69), ifm.searchXIB(encodeMeta(M2, 66, M3), M1).toSet());

        IntegerSeqResult tsi = ifm.searchXIB(encodeMeta(M2, 64, M3), M1);
        assertEquals(2, tsi.estimateSize(64));
        assertEquals(ZicoUtil.set(24, 42), tsi.toSet());

        ifm.close();
    }

    @Test
    public void testSearchXIB1Cmp() throws Exception {
        CompositeIndexStore store = new CompositeIndexFileStore(tmpDir, "test", ZicoUtil.props());
        CompositeIndex cti = new CompositeIndex(store, ZicoUtil.props(), Runnable::run);

        cti.add(encodeMeta(M1, 42, M2, 64, M3));
        cti.add(encodeMeta(M1, 3434, M2, 45, M3));
        // TODO fix this cti.rotate();
        cti.add(encodeMeta(M1, 24, M2, 64, M3));
        cti.add(encodeMeta(69, M2, 66, M3));


        assertEquals(ZicoUtil.set(3434), cti.searchXIB(encodeMeta(M2, 45, M3), M1).toSet());
        assertEquals(ZicoUtil.set(24, 42), cti.searchXIB(encodeMeta(M2, 64, M3), M1).toSet());
        assertEquals(ZicoUtil.set(69), cti.searchXIB(encodeMeta(M2, 66, M3), M1).toSet());

        IntegerSeqResult tsi = cti.searchXIB(encodeMeta(M2, 64, M3), M1);
        assertEquals(2, tsi.estimateSize(64));
        assertEquals(ZicoUtil.set(24, 42), tsi.toSet());

        cti.close();
    }

}
