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
