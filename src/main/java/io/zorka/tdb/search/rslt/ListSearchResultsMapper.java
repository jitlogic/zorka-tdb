package io.zorka.tdb.search.rslt;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ListSearchResultsMapper<T> implements SearchResultsMapper {

    private List<T> cached;
    private Function<T,SearchResult> mapFn;

    public ListSearchResultsMapper(List<T> data, Function<T,SearchResult> mapFn) {
        cached = new ArrayList<>(data);
        this.mapFn = mapFn;

    }

    @Override
    public SearchResult next() {
        if (cached.size() > 0) {
            SearchResult rslt = mapFn.apply(cached.get(0));
            cached.remove(0);
            return rslt;
        } else {
            return null;
        }
    }

    @Override
    public int size(int limit) {
        return cached.size();
    }
}
