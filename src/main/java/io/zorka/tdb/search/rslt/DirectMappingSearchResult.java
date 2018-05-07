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

