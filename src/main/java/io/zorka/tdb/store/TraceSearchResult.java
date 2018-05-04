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

package io.zorka.tdb.store;


import java.util.ArrayList;
import java.util.List;

/**
 * Search result returns series of local chunk IDs. Consumer code must then retrieve
 * data for each returned ID.
 */
public interface TraceSearchResult {

    /** Returns chunk ID of next matching result. */
    long getNext();

    default List<Long> drain() {
        List<Long> rslt = new ArrayList<>();
        for (long i = getNext(); i >= 0; i = getNext()) {
            rslt.add(i);
        }
        return rslt;
    }

}
