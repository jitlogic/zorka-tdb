package io.zorka.tdb.search;

import io.zorka.tdb.search.rslt.SearchResult;

/**
 * Searchable index or data store.
 */
public interface SearchableStore {


    SearchResult search(SearchNode expr);

    default int estimate(SearchNode expr, int limit) {
        return search(expr).estimateSize(limit);
    }
}
