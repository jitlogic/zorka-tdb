package io.zorka.tdb.store;

import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.meta.MetadataQuickIndex;
import io.zorka.tdb.meta.MetadataTextIndex;
import io.zorka.tdb.meta.StructuredTextIndex;
import io.zorka.tdb.search.QmiNode;
import io.zorka.tdb.search.SearchNode;
import io.zorka.tdb.search.TraceSearchQuery;
import io.zorka.tdb.search.lsn.AndExprNode;
import io.zorka.tdb.search.rslt.CascadingSearchResultsMapper;
import io.zorka.tdb.search.rslt.ConjunctionSearchResult;
import io.zorka.tdb.search.rslt.SearchResult;
import io.zorka.tdb.search.rslt.StreamingSearchResult;
import io.zorka.tdb.util.BitmapSet;
import io.zorka.tdb.util.KVSortingHeap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SimpleTraceStoreSearchResult implements TraceSearchResult {

    private final static Logger log = LoggerFactory.getLogger(SimpleTraceStoreSearchResult.class);

    private SimpleTraceStore store;

    private TraceSearchQuery query;
    private MetadataQuickIndex qindex;

    private MetadataTextIndex imeta;
    private StructuredTextIndex itext;

    private KVSortingHeap results;
    private Set<String> uuids;

    public SimpleTraceStoreSearchResult(TraceSearchQuery query, SimpleTraceStore store) {
        this.query = query;
        this.store = store;
        this.qindex = store.getQuickIndex();
        this.imeta = store.getMetaIndex();
        this.itext = store.getTextIndex();

        int limit = Integer.max(256, query.getLimit() * 4 + query.getOffset());

        results = new KVSortingHeap(limit, true);
        uuids = new HashSet<>();

        runSearch();

        if (query.getOffset() > 0) {
            skip(query.getOffset());
        }
    }

    private void runSearch() {
        int[] ids = new int[query.getBlkSize()];
        int[] vals = new int[query.getBlkSize()];
        int pos = qindex.size();
        int cnt;

        SearchNode expr = query.getNode();

        BitmapSet bmps = fullTextSearch(expr);
        if (bmps != null && log.isTraceEnabled()) {
            log.trace("bmps.count() = {}", bmps.count());
        }

        do {
            int start = Math.max(0, pos - query.getBlkSize());
            cnt = qindex.searchBlock(query.getQmi(), query.getSortOrder(), start, pos, ids, vals);
            for (int i = cnt - 1; i >= 0; i--) {
                if (bmps == null || bmps.get(ids[i])) {
                    results.add(ids[i], vals[i]);
                }
            }
            pos = start;
        } while (cnt >= 0 && pos > 0);

        results.invert();

    }

    private void skip(int n) {
        if (n > 0) {
            int c = n;
            while (c > 0) {
                int id = results.next();

                if (id >= 0) {
                    String uuid = store.getTraceUUID(id);
                    if (!uuids.contains(uuid)) {
                        uuids.add(uuid);
                        c -= 1;
                    }
                } else {
                    break;
                }
            }
        }
    }

    private BitmapSet fullTextSearch(SearchNode expr) {
        BitmapSet bmps = null;

        if (expr instanceof AndExprNode) {
            List<SearchResult> tsr = new ArrayList<>();
            for (SearchNode node : ((AndExprNode)expr).getArgs()) {
                tsr.add(tidTranslatingResult(itext.search(node), query.isDeepSearch()));
            }
            bmps = new ConjunctionSearchResult(tsr).getResultSet();
        } else if (expr != null && expr.getClass() != QmiNode.class) {
            bmps = tidTranslatingResult(itext.search(expr), query.isDeepSearch()).getResultSet();
        }
        return bmps;
    }

    private SearchResult tidTranslatingResult(SearchResult rslt, boolean deep) {
        CascadingSearchResultsMapper mapper = new CascadingSearchResultsMapper(rslt,
                tid -> imeta.searchIds(tid, deep));
        return new StreamingSearchResult(mapper);
    }

    private TraceSearchResultItem extractChunkMetadata(int slot) {
        long chunkId = store.toChunkId(slot);
        ChunkMetadata md = store.getChunkMetadata(chunkId);

        TraceSearchResultItem itm = new TraceSearchResultItem(chunkId, md);

        itm.setChunkId(chunkId);
        itm.setDescription(store.getDesc(chunkId));

        return itm;
    }

    @Override
    public TraceSearchResultItem nextItem() {
        for (int id = results.next(); id >= 0; id = results.next()) {
            String uuid = store.getTraceUUID(id);
            if (!uuids.contains(uuid)) {
                uuids.add(uuid);

                TraceSearchResultItem itm = extractChunkMetadata(id);
                itm.setUuid(uuid);

                return itm;
            }

            // TODO ponowne wykonanie runSearch() jeżeli jednak zabraknie danych wejściowych
        }

        return null;
    }

    @Override
    public int size() {
        return results.size();
    }

}
