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

package io.zorka.tdb.util;

import io.zorka.tdb.ZicoException;
import io.zorka.tdb.ZicoException;

/**
 * This is simple hash map implementation for WAL indexes. It is also mapped to a file, so it opens quickly.
 * File consists of two sections:
 * - ID -> position - simple array indexed by (id - idBase), contains positions of dictionary records;
 * - hash -> position - maps hashes of strings to positions of dictionary records;
 *
 * API of this map is tailored specificaly for WalIndexFile, it is not intended for general use.
 *
 */
public class QuickHashTab {

    public final static int MAX_STEPS = 128;

    /** Maximum map size (in items). */
    private final int maxSize;

    /** Mask used when accessing hash table */
    private final int hashMask;

    /** Used to block map from inserting new items (eg. while resizing). */
    private volatile boolean locked = false;

    /** Physical address of idx part (as mapped in JVM memory) */
    private volatile int[] byId;

    /** Physical address of hash part (as mapped in JVM memory). */
    private volatile long[] byHash;

    /** Number of entries in quick map */
    private volatile int numEntries;


    public QuickHashTab(int maxSize) {
        this.maxSize = maxSize;
        this.hashMask = maxSize * 2 - 1;

        byId = new int[maxSize];
        byHash = new long[maxSize * 2];
    }

    /**
     * Extends map capacity by factor of 2. Returns new map, does not change current one.
     *
     * @return extended map
     */
    public synchronized QuickHashTab extend() {
        locked = true;

        QuickHashTab rslt = new QuickHashTab(this.maxSize * 2);
        System.arraycopy(this.byId, 0, rslt.byId, 0, this.maxSize);

        rslt.numEntries = this.numEntries;

        for (long l : this.byHash) {
            if (l != 0) {
                int idx = (int) (l & rslt.hashMask);
                if (rslt.byHash[idx] == 0) {
                    rslt.byHash[idx] = l;
                } else {
                    for (int i = 1; i < MAX_STEPS; i++) {
                        int idx1 = (idx + i) & rslt.hashMask;
                        if (rslt.byHash[idx1] == 0) {
                            rslt.byHash[idx1] = l;
                            break;
                        }
                    }
                }
            }
        }

        return rslt;
    }


    /**
     * Adds info about new term
     * @param id term ID
     * @param hash hash of term string (XXHash32)
     * @param position term position in WAL file
     * @return true if item successfully added, false if lack of space occured;
     */
    public boolean put(int id, int hash, int position) {

        hash &= 0x7fffffff;  // Get rid of signedness

        if (id < 0) {
            throw new ZicoException("Invalid parameters");
        }
        synchronized (this) {

            if (locked) {
                return false;
            }

            if (id < maxSize) {
                byId[id] = position;
            } else {
                return false;
            }

            for (int i = 0; i < MAX_STEPS; i++) {
                long l = byHash[(hash+i) & hashMask];
                if (l == 0) {
                    byHash[(hash+i) & hashMask] = ((long)hash) | (((long)position) << 32);  // TODO this isn't atomic
                    numEntries++;
                    return true;
                }
            }
        }

        return false;
    }


    /**
     * Looks for
     * @param id local term ID;
     * @return position in WAL file
     */
    public int getById(int id) {
        if (id < 0) {
            throw new ZicoException("Invalid parameters");
        }
        if (id < maxSize) {
            return byId[id];
        } else {
            return -1;
        }
    }


    public long getByHash(int hash) {
        return getByHash(hash, 0);
    }

    public long getByHash(int hash, int idx) {
        hash &= 0x7fffffff;  // Get rid of signedness
        for (int i = idx; i < MAX_STEPS; i++) {
            long l = byHash[(hash + i) & hashMask];
            int h = (int) (l & 0xffffffffL);
            if (hash == h)
                return (l & 0xffffffff00000000L) | i;
        }
        return -1;
    }


    public synchronized void lock() {
        locked = true;
    }


    public int getMaxSize() {
        return maxSize;
    }
}
