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

package io.zorka.tdb.util;

import io.zorka.tdb.ZicoException;
import io.zorka.tdb.search.rslt.SearchResult;

import java.util.Arrays;

public class BitmapSet {

    private static final int LBITS = 10;  // minimum value: 6
    private final int L1LEN = 1 << (LBITS - 6);
    private static final int LMASK = (1 << LBITS) - 1;
    private static final int IMAX = 1024*1024*1024;

    private long[][] data;
    private int count = -1, imax = 0;

    // TODO add offset attribute

    public BitmapSet() {
        data = new long[16][];
    }

    public BitmapSet(int...ids) {
        this();
        for (int i : ids) {
            set(i);
        }
    }


    private BitmapSet(long[][] data, int imax) {
        this.imax = imax;
        this.data = data;
    }


    public void set(int idx) {
        count = -1;
        if (idx >= 0 && idx < IMAX) {
            int x0 = idx >>> LBITS, x1 = idx & LMASK, x2 = idx & 63;

            if (x0 >= data.length) {
                int l = data.length;
                while (l < x0) l <<= 1;
                data = Arrays.copyOf(data, l);
            }

            if (data[x0] == null) {
                data[x0] = new long[L1LEN];
            }

            data[x0][x1>>>6] |= (1L<<x2);
            imax = idx > imax ? idx : imax;
        } else {
            throw new ZicoException("Index " + idx + " not allowed.");
        }
    }


    public void del(int idx) {
        if (idx >= 0 && idx < IMAX) {
            int x0 = idx >>> LBITS, x1 = idx & LMASK, x2 = idx & 63;
            if (x0 < data.length && data[x0] != null) {
                data[x0][x1>>>6] &= ~(1L<<x2);
                count = -1;
            }
        } else {
            throw new ZicoException("Index " + idx + " not allowed.");
        }
    }


    public boolean get(int idx) {

        if (idx >= 0 && idx < IMAX) {
            int x0 = idx >>> LBITS, x1 = idx & LMASK, x2 = idx & 63;

            if (x0 >= data.length || data[x0] == null) return false;

            long d1 = data[x0][x1>>>6];

            return 0 != (d1&(1L<<x2));

        } else {
            throw new ZicoException("Index " + idx + " not allowed.");
        }
    }


    public BitmapSet and(BitmapSet arg) {
        long[][] dt = new long[Math.min(data.length, arg.data.length)][];
        for (int i = 0; i < dt.length; i++) {
            long[] d0 = data[i], d1 = arg.data[i];
            if (d0 != null && d1 != null) {
                long[] d = new long[L1LEN];
                for (int j = 0; j < L1LEN; j++) {
                    d[j] = d0[j] & d1[j];
                }
                dt[i] = d;
            }
        }
        return new BitmapSet(dt, imax);
    }


    public BitmapSet or(BitmapSet arg) {
        long[][] dt = new long[Math.max(data.length, arg.data.length)][];
        for (int i = 0; i < dt.length; i++) {
            long[] d0 = data[i], d1 = arg.data[i];
            if (d0 != null && d1 != null) {
                long[] d = new long[L1LEN];
                for (int j = 0; j < L1LEN; j++) {
                    d[j] = d0[j] | d1[j];
                }
                dt[i] = d;
            } else if (d0 != null || d1 != null) {
                dt[i] = Arrays.copyOf(d0 != null ? d0 : d1, L1LEN);
            }
        }
        return new BitmapSet(dt, imax);
    }


    public int count() {
        if (count < 0) {
            int c = 0;

            for (long[] d : data) {
                if (d != null) {
                    for (long l : d) {
                        if (l != 0) {
                            for (int b = 0; b < 64; b++) {
                                if (0 != (l & (1L << b))) {
                                    c++;
                                }
                            }
                        }
                    }
                }
            }

            count = c;
        }
        return count;
    }


    public int size() {
        return data.length << LBITS;
    }

    private class SearchIterator implements SearchResult {

        int cur = 0, cnt = 0;

        @Override
        public long nextResult() {
            while (cur <= imax && !get(cur)) cur++;
            int rslt = cur <= imax ? cur : -1;
            cur++; cnt++;
            return rslt;
        }

        @Override
        public int estimateSize(int limit) {
            return count() - cnt;
        }
    }

    public SearchResult searchAll() {
        return new SearchIterator();
    }
}
