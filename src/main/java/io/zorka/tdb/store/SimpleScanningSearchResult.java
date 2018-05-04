package io.zorka.tdb.store;

import io.zorka.tdb.meta.MetadataQuickIndex;
import io.zorka.tdb.meta.MetadataSearchQuery;
import io.zorka.tdb.meta.MetadataTextIndex;
import io.zorka.tdb.meta.StructuredTextIndex;
import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.text.re.StringBufView;
import io.zorka.tdb.util.IntegerSeqResult;
import io.zorka.tdb.meta.MetadataTextIndex;
import io.zorka.tdb.meta.StructuredTextIndex;
import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.util.IntegerSeqResult;

import java.util.BitSet;
import java.util.List;

/**
 * Scanning search result uses quick index as search base and checks each result
 * for full text indexes. This is optimized for searches where result set from quick
 * index is relatively small.
 */
public class SimpleScanningSearchResult implements TraceSearchResult {
    private SimpleTraceStore store;
    private MetadataQuickIndex qindex;
    private MetadataTextIndex imeta;
    private StructuredTextIndex itext;
    private IntegerSeqResult msr;
    private StoreSearchQuery query;
    private List<SearchPattern> patterns;

    public SimpleScanningSearchResult(SimpleTraceStore store, StoreSearchQuery query, List<SearchPattern> patterns) {
        this.store = store;
        this.query = query;

        this.qindex = store.getQuickIndex();
        this.imeta = store.getMetaIndex();
        this.itext = store.getTextIndex();

        this.msr = qindex.search(query);
        this.patterns = patterns;
    }

    private boolean matches(int slotId) {

        if (query.getPatterns() == null || query.getPatterns().size() == 0) return true;

        BitSet bs = new BitSet();

        int tid = query.hasSFlag(MetadataSearchQuery.DEEP_SEARCH) ? qindex.getFtid(slotId) : qindex.getTtid(slotId);
        if (tid > 0) {
            int[] ids = imeta.extractMetaTids(tid);
            for (int id : ids) {
                if (id == 0) continue; // TODO this should not happen
                byte[] b = itext.get(id);
                if (b == null) continue; // TODO this should not happen
                for (int i = 0; i < query.getPatterns().size(); i++) {
                    if (bs.get(i)) continue;
                    StringBufView v = new StringBufView(b, true);
                    int m = patterns.get(i).getRoot().match(v);
                    if (m > 0) {
                        bs.set(i);
                    }
                }
            }

            for (int i = 0; i < query.getPatterns().size(); i++) {
                if (!bs.get(i)) return false;
            }

        }

        return true;
    }

    @Override
    public long getNext() {

        int rslt;

        do {
            rslt = msr.getNext();
        } while (rslt != -1 && !matches(rslt));

        return  store.toChunkId(rslt);
    }
}
