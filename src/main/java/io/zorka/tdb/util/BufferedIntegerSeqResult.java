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

import java.util.Arrays;

@Deprecated
public class BufferedIntegerSeqResult implements IntegerSeqResult {

    private int[] results;
    private int nresults, cresult;  // number of results, current result (to be returned)

    private IntegerGetter sizeFn, nextFn;

    public BufferedIntegerSeqResult(IntegerGetter sizeFn, IntegerGetter nextFn) {
        this.sizeFn = sizeFn;
        this.nextFn = nextFn;
    }

    @Override
    public int estimateSize(int sizeMax) {
        int rlen = Math.min(sizeFn.get(), sizeMax);
        if (results != null) {
            results = Arrays.copyOfRange(results, cresult, cresult+rlen);
            nresults -= cresult; cresult = 0;
        } else {
            results = new int[rlen];
        }

        while (nresults < sizeMax) {
            int x = nextFn.get();
            if (x != -1) {
                results[nresults++] = x;
            } else {
                break;
            }
        }

        return nresults - cresult;
    }

    @Override
    public int getNext() {
        if (cresult < nresults) {
            return results[cresult++];
        }
        return nextFn.get();
    }
}
