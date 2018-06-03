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

package io.zorka.tdb.search.rslt;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Generalized search result wrapper for cases where input data item is transformed
 * to at most 1 output data item.
 */
public class DirectMappingSearchResult implements SearchResult {

    private SearchResult result;
    private Function<Long,Long> mapFn;
    private Set<Long> visited = new HashSet<>();

    public DirectMappingSearchResult(SearchResult result, Function<Long,Long> mapFn) {
        this.result = result;
        this.mapFn = mapFn;
    }

    @Override
    public long nextResult() {

        for (long r = result.nextResult(); r >= 0; r = result.nextResult()) {

            long rslt = mapFn.apply(r);

            if (visited.contains(r)) {
                continue;
            } else {
                visited.add(r);
            }

            if (rslt >= 0) return rslt;
        }

        return -1;
    }

    @Override
    public int estimateSize(int limit) {
        return result.estimateSize(limit);
    }
}

