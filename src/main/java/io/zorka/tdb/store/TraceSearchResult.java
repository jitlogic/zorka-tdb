package io.zorka.tdb.store;

import java.util.Iterator;

public interface TraceSearchResult extends Iterable<TraceSearchResultItem> {

    TraceSearchResultItem nextItem();

    int size();

    default Iterator<TraceSearchResultItem> iterator() {
        return new TraceSearchResultIterator(this);
    }

}
