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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Search result from text index. Objects implementing this interface are returned from text index search functions.
 */
public interface  IntegerSeqResult {

    /**
     * Estimates number of results. This will be
     * @param sizeMax maximum expected number of results, implementation will stop looking for new results when exceeds this
     * @return estimated number of results
     */
    int estimateSize(int sizeMax);

    /**
     * Returns next results of -1 if no more results are available
     * @return
     */
    int getNext();

    default Set<Integer> toSet() {
        Set<Integer> rslt = new HashSet<>();
        for (int x = getNext(); x != -1; x = getNext()) {
            rslt.add(x);
        }
        return rslt;
    }

    default List<Integer> toList() {
        List<Integer> rslt = new ArrayList<>();
        for (int x = getNext(); x != -1; x = getNext()) {
            rslt.add(x);
        }
        return rslt;
    }
}
