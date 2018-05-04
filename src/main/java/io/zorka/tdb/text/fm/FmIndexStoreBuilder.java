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

package io.zorka.tdb.text.fm;

import io.zorka.tdb.text.wal.WalTextIndex;
import io.zorka.tdb.text.wal.WalTextIndex;

import java.io.IOException;

public interface FmIndexStoreBuilder {
    void write(FmBlockDesc bdesc, byte[] data, int offs, int len) throws IOException;

    void finish(int nwords, long idbase, int pidx) throws IOException;

    default void bwtToFm(byte[] data, int nwords, long idbase, int pidx) throws IOException {
        bwtToFm(data, 0, data.length, nwords, idbase, pidx);
    }

    default void bwtToFm(byte[] data, int offs, int len, int nwords, long idbase, int pidx) throws IOException {
        write(null, data, offs, len);
        finish(nwords, idbase, pidx);
    }

    /**
     * Constructs FM index based on raw WAL index.
     * @param wal WAL index object
     * @throws IOException
     */

    default void walToFm(WalTextIndex wal) throws IOException {
        int nwords = (int)wal.getNWords();
        long idBase = wal.getIdBase();

        byte[] data = wal.getData(true, true, true);
        int[] ibuf = new int[data.length];
        byte[] bbuf = new byte[data.length];
        int pidx = BWT.bwtencode(data, bbuf, ibuf, data.length);

        ibuf = null; data = null; // Make sure GC can get rid of these two big blocks

        bwtToFm(bbuf, 0, bbuf.length, nwords, idBase, pidx);
    }


    default void rawToFm(byte[] data, int nwords, long idBase) throws IOException {
        int[] ibuf = new int[data.length];
        byte[] bbuf = new byte[data.length];
        int pidx = BWT.bwtencode(data, bbuf, ibuf, data.length);
        ibuf = null;
        bwtToFm(bbuf, 0, bbuf.length, nwords, idBase, pidx);
    }
}
