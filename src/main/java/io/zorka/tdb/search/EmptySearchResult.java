package io.zorka.tdb.search;

import io.zorka.tdb.search.rslt.SearchResult;

public class EmptySearchResult implements SearchResult {

    public final static EmptySearchResult INSTANCE = new EmptySearchResult();

    @Override
    public long nextResult() {
        return -1;
    }

    @Override
    public int estimateSize(int limit) {
        return 0;
    }
}
