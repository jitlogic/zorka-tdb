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

import io.zorka.tdb.ZicoException;
import io.zorka.tdb.text.wal.WalTextIndex;
import io.zorka.tdb.text.wal.WalTextIndex;

import java.io.IOException;

import static io.zorka.tdb.text.fm.FmIndexStore.*;

/**
 * Mutable FM index store that consists of immutable file-backed store and in-memory patch on top of it
 */
public class FmIndexPatchStore implements FmIndexStore {

    /** Backing store */
    private FmIndexFileStore bstore;

    private FmPatchNode root;

    private int pidx;

    public FmIndexPatchStore(FmIndexFileStore bstore) {
        this.bstore = bstore;
        this.root = new FmPatchNodeL(bstore.getDatalen());
        this.pidx = bstore.getPidx();
    }


    @Override
    public long charAndRank(int pos) {

        long car = root.charAndRank(pos);

        if (car != -1) {
            byte ch = chr(car);
            int r1 = rnk(car);
            int r2 = bstore.rankOf(pos - root.size(pos), ch);
            return car(ch, r1 + r2);
        } else {
            car = bstore.charAndRank(pos - root.size(pos));
            byte ch = chr(car);
            int r1 = rnk(car);
            int r2 = root.rankOf(pos, ch);
            return car(ch, r1 + r2);
        }
    }


    private int c2u(byte ch) {
        return ch & 0xff;
    }

    public void append(WalTextIndex wal) {
        append(wal.getData(true, true, true));
    }


    /**
     * Appends some data
     * @param data data (as byte array)
     */
    public void append(byte[] data) {
        append(data, 0, data.length);
    }

    public void append(byte[] data, int offs, int len) {

        long car = charAndRank(pidx);

        System.out.println("CH0=" + chr(car));

        byte tChr = chr(car);
        pidx = getCharOffs(tChr) + rankOf(pidx, tChr);

        insert(pidx, tChr);

        for (int i = len+offs-2; i >= offs; i--) {
            byte c = data[i];

            int nidx = getCharOffs(c) + rankOf(pidx, c);

            if (replace(pidx, c) == -1) {
                throw new ZicoException("Trying to replace non-existent character at pos " + pidx);
            }

            pidx = nidx;

            insert(pidx, tChr);
        }

        System.out.println("CH1=" + chr(charAndRank(pidx)));
    }


    /**
     * Insert single character into specified position.
     * @param pos position
     * @param ch character
     */
    public void insert(int pos, byte ch) {
        root = root.insert(pos, ch);
    }

    public int replace(int pos, byte ch) {
        return root.replace(pos, ch);
    }


    /** Dump buffer size. */
    private final static int DUMP_BUFSZ = 16 * 1024 * 1024;


    public void dump(FmIndexStoreBuilder isb, int startpos, boolean clean) throws IOException {
        dump(isb, startpos, DUMP_BUFSZ, clean);
    }


    /**
     * Creates new merged FM index from data inside backing store and patch data.
     * @param isb index store builder that will receive dumped data (and build
     * @param clean if true, method will clean old buffered data as soon as possible in order to relieve memory and GC
     */
    public void dump(FmIndexStoreBuilder isb, int startpos, int blocksz, boolean clean) throws IOException {
        long[] patches = new long[root.size()];

        if (clean) root = null;

        byte[] ibuf = new byte[blocksz];
        int ipos = 0, ilen = 0; // Position in input and output buffers
        byte[] obuf = new byte[blocksz];
        int opos = 0;
        int lpos = startpos, llen = getDatalen();   // Logical position and length (in merged string)
        int bpos = 0, ppos = 0;                     // Logical position in backing store and patch buffer

        // Move to start postion in patches array
        while (ppos < patches.length && rnk(patches[ppos]) < startpos) ppos++;

        int plen = root.getRawData(patches);

        while (lpos < llen) {

            if (ipos >= ilen) {   // Load some more input data if needed
                ilen = bstore.getData(ibuf, bpos);
                bpos += ilen;
                ipos = 0;
            }

            int lpos0 = lpos, pp0 = ppos < patches.length ? rnk(patches[ppos]) : -1;

            while (ppos < plen && opos < blocksz && lpos == pp0) {
                obuf[opos++] = chr(patches[ppos++]); lpos++;
                pp0 = ppos < patches.length ? rnk(patches[ppos]) : -1;
            }

            while (opos < blocksz && ipos < ilen && lpos < llen && (pp0 == -1 || lpos < pp0)) {
                int len = Math.min(Math.min(ilen - ipos, blocksz - opos), pp0 != -1 ? pp0-lpos : llen-lpos);
                if (len > 1) {
                    System.arraycopy(ibuf, ipos, obuf, opos, len);
                } else {
                    obuf[opos] = ibuf[ipos];
                }
                ipos += len; opos += len; lpos += len;
            }

            if (lpos0 == lpos) {
                throw new ZicoException("Internal error: patch merging didn't progress: LLEN=" + llen + " LPOS=" + lpos
                        + " IPOS=" + ipos + " OPOS=" + opos + " PPOS=" + ppos + " PLEN=" + plen);
            }

            if (opos == blocksz) {
                isb.write(null, obuf, 0, opos);
                opos = 0;
            }
        } // while ()

        if (opos > 0) {
            isb.write(null, obuf, 0, opos);
        }

        if (clean) {
            ibuf = obuf = null;
            patches = null;
        }

        isb.finish(bstore.getNWords(), bstore.getIdBase(), pidx);  // TODO proper nwords and pidx calculation
    }


    @Override
    public int rankOf(int pos, byte chr) {
        return root.rankOf(pos, chr) + bstore.rankOf(pos - root.size(pos), chr);
    }


    @Override
    public int getCharOffs(byte ch) {
        return bstore.getCharOffs(ch) + root.getCharOffs(ch);
    }


    @Override
    public long getNBlocks() {
        return bstore.getNBlocks();
    }


    @Override
    public int getDatalen() {
        return bstore.getDatalen() + root.size();
    }


    @Override
    public int getIdBase() {
        return bstore.getIdBase();
    }


    @Override
    public int getNWords() {
        return bstore.getNWords();
    }

    @Override
    public int getPidx() {
        return bstore.getPidx();
    }


    @Override
    public int getData(byte[] buf, int lpos) {
        FmIndexByteArrayStoreBuilder b = new FmIndexByteArrayStoreBuilder(buf);

        try {
            dump(b, lpos, false);
        } catch (IOException e) {
            throw new ZicoException("I/O error.", e);
        }

        return b.getPos();
    }

    @Override
    public long length() {
        return 0L;
    }


    @Override
    public void close() throws IOException {
        bstore.close();
    }

}
