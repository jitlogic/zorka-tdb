package io.zorka.tdb.store;

import java.util.Iterator;

public class TraceSearchResultIterator implements Iterator<TraceSearchResultItem> {

    private TraceSearchResult rslt;
    private TraceSearchResultItem item;

    public TraceSearchResultIterator(TraceSearchResult rslt) {
        this.rslt = rslt;
        this.item = rslt.nextItem();
    }

    @Override
    public boolean hasNext() {
        return item != null;
    }

    @Override
    public TraceSearchResultItem next() {
        TraceSearchResultItem itm = this.item;
        this.item = rslt.nextItem();
        return itm;
    }


}
