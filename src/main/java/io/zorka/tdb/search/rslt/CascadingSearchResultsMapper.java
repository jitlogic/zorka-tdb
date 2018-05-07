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
