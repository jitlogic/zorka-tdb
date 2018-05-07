package io.zorka.tdb.search.rslt;

public interface SearchResultsMapper {

    SearchResult next();


    int size(int limit);
}
