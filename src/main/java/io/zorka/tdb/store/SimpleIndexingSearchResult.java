package io.zorka.tdb.store;

import io.zorka.tdb.meta.MetadataSearchQuery;
import io.zorka.tdb.meta.MetadataTextIndex;
import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.util.IntegerSeqResult;
import io.zorka.tdb.meta.MetadataTextIndex;
import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.util.IntegerSeqResult;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Indexing search result uses full text indexes as search base. This is suitable for
 * cases where quick index result set is relatively big.
 */
public class SimpleIndexingSearchResult implements TraceSearchResult {

    private SimpleTraceStore store;
    private MetadataTextIndex imeta;

    private IntegerSeqResult msr;
    private StoreSearchQuery query;
    private List<SearchPattern> patterns;

    private List<IntegerSeqResult> tsr;
    private List<Set<Integer>> textIds = new ArrayList<>();
    private List<Set<Integer>> metaIds = new ArrayList<>();


    public SimpleIndexingSearchResult(SimpleTraceStore store, StoreSearchQuery query,
                                      IntegerSeqResult msr, List<IntegerSeqResult> tsr,
                                      List<SearchPattern> patterns) {
        this.store = store;
        this.imeta = store.getMetaIndex();
        this.msr = msr;
        this.query = query;
        this.tsr = tsr;
        this.patterns = patterns;
        lookupTextIds();
        lookupMetaIds();
    }

    private void lookupTextIds() {
        if (patterns != null) {
            for (IntegerSeqResult sr : tsr) {
                Set<Integer> ts = new HashSet<>();
                for (int id = sr.getNext(); id != -1; id = sr.getNext()) {
                    ts.add(id);
                }
                textIds.add(ts);
            }
        }
    }

    private void lookupMetaIds() {
        for (Set<Integer> ts : textIds) {
            Set<Integer> ms = new HashSet<>();
            for (Integer id : ts) {
                boolean deep = 0 != (query.getSflags() & MetadataSearchQuery.DEEP_SEARCH);
                Set<Integer> coll = imeta.findByIds(id, deep).toSet();
                ms.addAll(coll);
            }
            metaIds.add(ms);
        }
    }


    @Override
    public long getNext() {
        int rslt = msr.getNext();

        if (patterns.size() > 0) {
            while (rslt >= 0 && !metaIds.get(0).contains(rslt)) {  // TODO uwzględnić więcej fraz
                rslt = msr.getNext();
            }
        }

        return store.toChunkId(rslt);
    }

}
