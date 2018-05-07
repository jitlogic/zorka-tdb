package io.zorka.tdb.search.rslt;

import io.zorka.tdb.search.EmptySearchResult;
import io.zorka.tdb.util.BitmapSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConjunctionSearchResult implements SearchResult {

    private List<SearchResult> inputs;
    private SearchResult result;

    public ConjunctionSearchResult(SearchResult sr1, SearchResult sr2) {
        inputs = Arrays.asList(sr1, sr2);
        initSearch();
    }

    public ConjunctionSearchResult(List<SearchResult> inputs) {
        this.inputs = new ArrayList<>(inputs);
        initSearch();
    }

    private void initSearch() {
        BitmapSet bmps = null;

        for (SearchResult sr : inputs) {
            BitmapSet b = new BitmapSet();
            for (long l = sr.nextResult(); l >= 0; l = sr.nextResult()) {
                b.add((int)l);
            }
            if (bmps == null) {
                bmps = b;
            } else {
                bmps = bmps.and(b);
            }
        }

        result = bmps != null ? bmps.searchAll() : EmptySearchResult.INSTANCE;
    }

    @Override
    public long nextResult() {
        return result.nextResult();
    }

    @Override
    public int estimateSize(int limit) {
        return result.estimateSize(limit);
    }
}
