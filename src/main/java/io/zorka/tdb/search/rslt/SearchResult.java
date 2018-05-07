package io.zorka.tdb.search.rslt;

public interface SearchResult {

    long nextResult();

    int estimateSize(int limit);

}
