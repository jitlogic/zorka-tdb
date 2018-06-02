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

package io.zorka.tdb.search.rslt;

import java.util.Collection;

public class CachedItemsSearchResult implements SearchResult {

    private long[] results;
    private int idx;

    public CachedItemsSearchResult(Collection<Long> coll) {
        results = new long[coll.size()];
        int x = 0;
        for (Long l : coll) {
            results[x++] = l;
        }
    }

    public CachedItemsSearchResult(long result) {
        this.results = new long[] { result };
    }

    @Override
    public long nextResult() {
        return idx < results.length ? results[idx++] : -1;
    }

    @Override
    public int estimateSize(int limit) {
        return results.length - idx;
    }
}
