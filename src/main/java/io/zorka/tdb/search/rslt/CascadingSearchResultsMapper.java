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

public class CascadingSearchResultsMapper implements SearchResultsMapper {

    private SearchResult sr;
    private Function<Long,SearchResult> mapFn;

    public CascadingSearchResultsMapper(SearchResult sr, Function<Long,SearchResult> mapFn) {
        this.sr = sr;
        this.mapFn = mapFn;
    }

    @Override
    public SearchResult next() {
        long r = sr.nextResult();
        return r >= 0 ? mapFn.apply(r) : null;
    }

    @Override
    public int size(int limit) {
        return sr.estimateSize(limit);
    }
}
