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

import java.util.function.Function;

public class MappingSearchResult implements SearchResult {

    private SearchResult result;
    private Function<Long,Long> mapFn;

    public MappingSearchResult(SearchResult result, Function<Long,Long> mapFn) {
        this.result = result;
        this.mapFn = mapFn;
    }

    @Override
    public long nextResult() {
        long r = result.nextResult();
        if (r >= 0) {
            return mapFn.apply(r);
        } else {
            return -1;
        }
    }

    @Override
    public int estimateSize(int limit) {
        return result.estimateSize(limit);
    }
}
