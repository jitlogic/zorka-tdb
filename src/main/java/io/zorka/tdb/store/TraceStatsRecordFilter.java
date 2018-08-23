package io.zorka.tdb.store;

import io.zorka.tdb.meta.StructuredTextIndex;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class TraceStatsRecordFilter implements TraceRecordFilter<Object> {

    private Map<Integer,TraceStatsResultItem> stats = new HashMap<>();

    @Override
    public Object filter(TraceRecord tr, StructuredTextIndex resolver) {

        TraceStatsResultItem itm = stats.get(tr.getMid());

        if (itm != null) {
            itm.add(tr);
        } else {
            itm = new TraceStatsResultItem(tr);
            itm.setMethod(resolver.resolve(tr.getMid()));
            stats.put(tr.getMid(), itm);
        }

        return this;
    }

    public Collection<TraceStatsResultItem> getStats() {
        return stats.values();
    }

}
