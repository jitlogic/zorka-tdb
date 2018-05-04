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

import java.util.Arrays;

/**
 * Node of FM patch tree.
 */
public abstract class FmPatchNode {

    protected int length;    // Patch node length

    public FmPatchNode(int length) {
        this.length = length;
    }


    /**
     * Returns logical length of BWT string this patch  
     * @return
     */
    public int length() {
        return length;
    }


    /**
     * Returns number of inserted characters in this patch.
     * @return number of characters
     */
    public int size() {
        return size(length);
    }


    /**
     * Returns number of inserted characters up to (but excluding) given position.
     * @param pos logical position (in patched BWT)
     * @return number of characters (counted only in patch)
     */
    public abstract int size(int pos);


    /**
     * Returns rank of given character at given position (only inserted characters are counted).
     * @param pos logical position (in patched BWT)
     * @param ch ranked character
     * @return rank (counted only in patch)
     */
    public abstract int rankOf(int pos, byte ch);

    /**
     * Returns character and its rank from patch (if any).
     * @param pos logical position (in patched BWT)
     * @return character rank (bits 0-31) and character (bits 32-39) or -1.
     */
    public abstract long charAndRank(int pos);


    /**
     * Adds new character at given position in patch. 
     * @param pos logical position (in patched BWT)
     * @param ch inserted character
     * @return modified patch node (mostly callee object)
     */
    public abstract FmPatchNode insert(int pos, byte ch);

    /**
     * Replaces character already inserted into patch.
     * @param pos character position
     * @param ch new character
     * @return old character or -1 if no character found
     */
    public abstract int replace(int pos, byte ch);

    /**
     * Returns offset where given character starts in sorted BWT string.
     * @param ch character
     * @return offset
     */
    public abstract int getCharOffs(byte ch);

    /**
     * Returns raw patch data encoded as (sortable) long[] integers. Use chr() and rnk() to extract character and position.
     * @param buf output buffer
     * @return number of chr-pos pairs returned
     */
    public int getRawData(long[] buf) {
        int rslt = getRawData(0, buf, 0);
        Arrays.sort(buf, 0, rslt);
        return rslt;
    }

    protected abstract int getRawData(int pos, long[] buf, int offs);
}
