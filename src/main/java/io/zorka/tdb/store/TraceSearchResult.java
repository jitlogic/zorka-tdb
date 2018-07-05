package io.zorka.tdb.store;

public interface TraceSearchResult {

    TraceSearchResultItem nextItem();

    int size();
}
