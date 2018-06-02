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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StreamingSearchResult implements SearchResult {

    private SearchResultsMapper results;
    private List<SearchResult> cache;
    private SearchResult result;
    private Set<Long> visited = new HashSet<>();

    public StreamingSearchResult(SearchResultsMapper results) {
        this.results = results;
        cache = new ArrayList<>();
        result = nextSR();
    }

    private SearchResult nextSR() {
        if (cache.size() > 0) {
            SearchResult rslt = cache.get(0);
            cache.remove(0);
            return rslt;
        }
        return results.next();
    }

    @Override
    public long nextResult() {
        while (result != null) {
            long r = result.nextResult();
            if (r >= 0 && visited.contains(r)) continue;
            if (r >= 0) {
                visited.add(r);
                return r;
            }
            result = nextSR();
        }
        return -1;
    }

    @Override
    public int estimateSize(int limit) {
        int rslt = result != null ? result.estimateSize(limit) : 0;

        if (rslt >= limit) return rslt;

        for (SearchResult sr : cache) {
            rslt += sr.estimateSize(limit-rslt);
            if (rslt >= limit) return rslt;
        }

        while (rslt < limit) {
            SearchResult sr = results.next();
            if (sr != null) {
                cache.add(sr);
                rslt += sr.estimateSize(limit);
            } else {
                break;
            }
        }

        return rslt;
    }
}
