package io.zorka.tdb.store;


import io.zorka.tdb.search.TraceSearchQuery;

import java.util.List;

public class RotatingTraceStoreSearchResult implements TraceSearchResult {

    private TraceSearchQuery query;
    private List<SimpleTraceStore> stores;
    private TraceSearchResult rslt;

    public RotatingTraceStoreSearchResult(TraceSearchQuery query, List<SimpleTraceStore> stores) {
        this.query = query;
        this.stores = stores;
        nextStore();
    }

    private void nextStore() {
        rslt = null;
        if (!stores.isEmpty()) {
            TraceStore store = stores.get(stores.size()-1);
            stores.remove(stores.size()-1);
            rslt = store.searchTraces(query);
        }
    }

    @Override
    public TraceSearchResultItem nextItem() {
        TraceSearchResultItem itm = null;

        while (rslt != null && itm == null) {
            itm = rslt.nextItem();
            if (itm == null) {
                nextStore();
            }
        }

        return itm;
    }

    @Override
    public int size() {
        return rslt != null ? rslt.size() : 0;
    }

}
