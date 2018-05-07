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
