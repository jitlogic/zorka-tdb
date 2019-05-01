package io.zorka.tdb.store;

import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.meta.MetadataQuickIndex;
import io.zorka.tdb.meta.MetadataTextIndex;
import io.zorka.tdb.meta.StructuredTextIndex;
import io.zorka.tdb.search.QmiNode;
import io.zorka.tdb.search.SearchNode;
import io.zorka.tdb.search.TraceSearchQuery;
import io.zorka.tdb.search.lsn.AndExprNode;
import io.zorka.tdb.util.BitmapSet;
import com.jitlogic.zorka.common.util.KVSortingHeap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SimpleTraceStoreSearchResult implements TraceSearchResult {

    private final static Logger log = LoggerFactory.getLogger(SimpleTraceStoreSearchResult.class);

    private SimpleTraceStore store;

    private TraceSearchQuery query;
    private QmiNode qmi;
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

        this.qmi =  query.getQmi();

        if (qmi == null) {
            qmi = new QmiNode();
        }

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
            cnt = qindex.searchBlock(qmi, query.getSortOrder(), start, pos, ids, vals);
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
                    ChunkMetadata md = store.getChunkMetadata(id);
                    String uuid = md.getTraceIdHex() + md.getSpanIdHex();
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
        List<SearchNode> args;

        if (expr instanceof AndExprNode) {
            args = ((AndExprNode) expr).getArgs();
        } else if (expr != null) {
            args = Collections.singletonList(expr);
        } else {
            args = Collections.emptyList();
        }

        for (SearchNode node : args) {
            BitmapSet bbs = new BitmapSet();
            itext.search(node, bbs);
            BitmapSet bps = new BitmapSet();

            // TODO further optimizations here: switch to matching mode (full scan) if there are too many TID results;

            for (int tid = bbs.first(); tid >= 0; tid = bbs.next(tid)) {
                imeta.searchIds(tid, query.isDeepSearch(), bps);
            }

            bmps = (bmps == null) ? bps : bmps.and(bps);
        }

        return bmps;
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
            ChunkMetadata md = store.getChunkMetadata(id);
            String uuid = md.getTraceIdHex() + md.getSpanIdHex();
            if (!uuids.contains(uuid)) {
                uuids.add(uuid);

                return extractChunkMetadata(id);
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
