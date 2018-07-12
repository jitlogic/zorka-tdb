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

import java.util.function.Function;

public class MappingSearchResult implements TextSearchResult {

    private TextSearchResult result;
    private Function<Integer,Integer> mapFn;

    public MappingSearchResult(TextSearchResult result, Function<Integer,Integer> mapFn) {
        this.result = result;
        this.mapFn = mapFn;
    }

    @Override
    public int nextResult() {
        int r = result.nextResult();
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
