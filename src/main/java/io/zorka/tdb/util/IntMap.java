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

/**
 * Simplified int-int hash map used in some parts. Does not implement most of standard
 * functionality nor java.util.Map interface. Only positive integers are handled.
 *
 * Designed to have low overhead on big data sets. Do not use in for general problems.
 */
public class IntMap {

    private final static int MAX_ATTEMPTS = 256;

    public static int get(long[] data, int k) {
        long m = data.length - 1;

        for (long i = 0; i < MAX_ATTEMPTS; i++) {
            int idx = (int)((k+i) & m);

            long d = data[idx];

            if (d == 0) return -1;

            if (k == (int)(d & 0xffffffffL))
                return (int)(d >>> 32);
        }
        return -1;
    }

    public static int put(long[] data, int k, int v) {
        long m = data.length - 1;
        for (int i = 0; i < MAX_ATTEMPTS; i++) {
            int idx = (int)((k+i) & m);
            long d = data[idx];
            if (d == 0 || k == (int)(d & 0xffffffffL)) {
                data[idx] = (long)k | (((long)v) << 32);
                return idx;
            }
        }
        return -1;
    }

    public static long[] extend(long[] odata, int size) {
        long[] ndata = new long[size];
        for (long l : odata) {
            int k = (int)(l & 0xffffffffL), v = (int) (l >>> 32);
            if (put(ndata, k, v) == -1) {
                System.out.println("Extension failed. Extending once more (should not happen).");
                return extend(odata, size * 2);
            }
        }
        return ndata;
    }

    private long[] data;  // Lower half = key, higher half = value

    public IntMap() {
        this(65536);
    }

    public IntMap(int size) {
        data = new long[size];
    }

    public int get(int k) {
        return get(data, k);
    }

    public void put(int k, int v) {
        while (put(data, k, v) == -1) {
            data = extend(data, data.length*2);
        }
    }

    public int datalen() {
        return data.length;
    }

    public int size() {
        int rslt = 0;

        for (long d : data) {
            if (d != 0) rslt++;
        }

        return rslt;
    }
}
