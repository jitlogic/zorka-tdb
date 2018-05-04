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

import static io.zorka.tdb.text.fm.FmIndexStore.car;

/**
 * Internal node of FM patch tree.
 */
public class FmPatchNodeI extends FmPatchNode {

    public final static int ISIZE = 16;

    private FmPatchNode[] nodes;  // subordinate nodes (ISIZE elements)
    private int[]         ranks;  // rank table        (256 elements)
    private int size = 0;


    public FmPatchNodeI(int length) {
        super(length);
        nodes  = new FmPatchNode[ISIZE];
        ranks  = new int[256];
        int chunk = (length+ISIZE-1) / ISIZE;
        for (int i = 0; i < ISIZE; i++) {
            nodes[i] = new FmPatchNodeL(chunk);
        }
        //nodes[63] = new FmPatchNodeL(length - 63 * chunk);
    }

    FmPatchNodeI(FmPatchNode[] nodes, int[] ranks, int size, int length) {
        super(length);
        this.nodes = nodes;
        this.ranks = ranks;
        this.size = size;
    }


    @Override
    public int size(int pos) {
        int rslt = 0, cur = 0;

        if (pos >= length) {
            return size;
        }

        for (int i = 0; i < ISIZE && cur < pos; i++) {
            rslt += nodes[i].size(pos - cur);
            cur  += nodes[i].length;
        }

        return rslt;
    }


    @Override
    public int rankOf(int pos, byte ch) {
        int rslt = 0, cur = 0;

        if (pos >= length) {
            return ranks[ch & 0xff];
        }

        for (int i = 0; i < ISIZE && cur < pos; i++) {
            rslt += nodes[i].rankOf(pos - cur, ch);
            cur += nodes[i].length;
        }

        return rslt;
    }


    @Override
    public long charAndRank(int pos) {
        long rslt = -1;

        if (pos >= length) {
            return rslt;
        }

        int cur = 0;

        for (int i = 0; i < ISIZE; i++) {
            int l = nodes[i].length;
            if (pos >= cur && pos < cur + l) {
                long car = nodes[i].charAndRank(pos - cur);
                if (car != -1) {
                    byte ch = FmIndexStore.chr(car);
                    int rank = FmIndexStore.rnk(car);
                    for (int j = 0; j < i; j++) {
                        rank += nodes[j].rankOf(pos, ch);
                    }
                    return FmIndexStore.car(ch, rank);
                } else {
                    return -1;
                }
            }
            cur += l;
        }

        return rslt;
    }


    @Override
    public FmPatchNode insert(int pos, byte ch) {

        if (pos > length) {
            throw new ZicoException("Cannot insert char at pos=" + pos + " (len=" + length + ")");
        }
        
        int cur = 0;
        for (int i = 0; i < ISIZE; i++) {
            int l = nodes[i].length;
            if (pos >= cur && pos < cur + l) {
                nodes[i] = nodes[i].insert(pos - cur, ch);
                break;
            }
            cur += l;
        }
        length++;
        size++;
        ranks[ch & 0xff]++;
        return this;
    }

    @Override
    public int replace(int pos, byte ch) {
        if (pos > length) {
            throw new ZicoException("Cannot replace char at pos=" + pos + " (len=" + length + ")");
        }

        int cur = 0;
        for (int i = 0; i < ISIZE; i++) {
            int l = nodes[i].length;
            if (pos >= cur && pos < cur + l) {
                int rslt = nodes[i].replace(pos - cur, ch);
                if (rslt != -1) {
                    ranks[rslt]--;
                    ranks[ch & 0xff]++;
                }
                return rslt;
            }
            cur += l;
        }

        return -1;
    }


    @Override
    public int getCharOffs(byte ch) {
        int c = ch & 0xff, rslt = 0;

        for (int i = 0; i < c; i++) {
            rslt += ranks[i];
        }

        return rslt;
    }


    @Override
    protected int getRawData(int pos, long[] buf, int offs) {
        int cur = 0, rslt = offs;
        for (int i = 0; i < ISIZE && rslt < buf.length; i++) {
            int l = nodes[i].length;
            rslt = nodes[i].getRawData(pos + cur, buf, rslt);
            cur += l;
        }
        return rslt;
    }
    
}
