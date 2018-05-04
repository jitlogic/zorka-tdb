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
import io.zorka.tdb.ZicoException;

import static io.zorka.tdb.text.fm.FmIndexStore.car;
import static io.zorka.tdb.text.fm.FmPatchNodeI.ISIZE;

/**
 * Leaf node of FM patch tree.
 */
public class FmPatchNodeL extends FmPatchNode {

    private final static int LSIZE = 64;

    private int size = 0;

    private final int[]  ptab = new int[LSIZE];
    private final byte[] ctab = new byte[LSIZE];


    public FmPatchNodeL(int length) {
        super(length);
    }


    @Override
    public int size(int pos) {

        if (pos >= length) {
            return size;
        }

        int rslt = 0;

        for (int i = 0; i < size; i++) {
            if (ptab[i] < pos) {
                rslt++;
            }
        }
        
        return rslt;
    }


    public int rankOf(int pos, byte ch) {
        int rslt = 0;

        for (int i = 0; i < size; i++) {
            if (ctab[i] == ch && ptab[i] < pos) {
                rslt++;
            }
        }

        return rslt;
    }

    @Override
    public long charAndRank(int pos) {
        int r = 0;
        for (int i = 0; i < size; i++) {
            if (pos == ptab[i]) {
                byte ch = ctab[i];
                for (int j = 0; j < size; j++) {
                    if (ctab[j] == ch && ptab[j] < pos) {
                        r++;
                    }
                }
                return FmIndexStore.car(ch, r);

            }
        }
        return -1;
    }


    @Override
    public FmPatchNode insert(int pos, byte ch) {

        if (pos > length) {
            throw new ZicoException("Cannot insert node at pos=" + pos + " (len=" + length + ")");
        }

        if (size < LSIZE) {
            // Add locally

            for (int i = 0; i < size; i++) {
                if (ptab[i] >= pos) {
                    ptab[i]++;
                }
            }

            ptab[size] = pos;
            ctab[size] = ch;
            size++;
            length++;
            return this;
        } else {
            // Split node into parts
            return split().insert(pos, ch);
        }
    }

    @Override
    public int replace(int pos, byte ch) {
        for (int i = 0; i < size; i++) {
            if (ptab[i] == pos) {
                int rslt = ctab[i] & 0xff;
                ctab[i] = ch;
                return rslt;
            }
        }
        return -1;
    }


    public FmPatchNode split() {
        // TODO przenieść to do konstruktora FmPatchNodeI
        FmPatchNode[] nodes  = new FmPatchNode[FmPatchNodeI.ISIZE];
        int[] ranks  = new int[256];
        int chunk = (length+ FmPatchNodeI.ISIZE-1) / FmPatchNodeI.ISIZE;

        for (int i = 0; i < FmPatchNodeI.ISIZE; i++) {
            nodes[i] = new FmPatchNodeL(chunk);
        }

        for (int i = 0; i < size; i++) {
            byte ch = ctab[i];
            int pos = ptab[i];
            ranks[ch & 0xff]++;
            FmPatchNodeL node = (FmPatchNodeL)nodes[pos / chunk];
            node.ptab[node.size] = pos % chunk;
            node.ctab[node.size] = ch;
            node.size += 1;
        }

        return new FmPatchNodeI(nodes, ranks, size, length);
    }
    

    @Override
    public int getCharOffs(byte ch) {
        int rslt = 0;

        int c = ch & 0xff;

        for (int i = 0; i < size; i++) {
            if ((ctab[i] & 0xff) < c) {
                rslt++;
            }
        }

        return rslt;
    }

    @Override
    protected int getRawData(int pos, long[] buf, int offs) {
        int sz = offs;
        for (int i = 0; i < size && sz < buf.length; i++) {
            buf[sz++] = FmIndexStore.car(ctab[i], ptab[i] + pos);
        }
        return sz;
    }


    @Override
    public String toString() {
        return "FmNodeL{size=" + size + ", length=" + length + ")";
    }
}
