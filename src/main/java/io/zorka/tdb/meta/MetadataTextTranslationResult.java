package io.zorka.tdb.meta;

import io.zorka.tdb.search.rslt.SearchResult;

public class MetadataTextTranslationResult implements SearchResult {

    //public

    @Override
    public long nextResult() {
        return 0;
    }

    @Override
    public int estimateSize(int limit) {
        return 0;
    }
}
