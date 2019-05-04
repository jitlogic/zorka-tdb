package io.zorka.tdb.store;

import io.zorka.tdb.text.StructuredTextIndex;
import io.zorka.tdb.search.QmiNode;
import io.zorka.tdb.search.TraceSearchQuery;
import org.mapdb.Fun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentNavigableMap;

public class SimpleTraceStoreSearchResult implements TraceSearchResult {

    private final static Logger log = LoggerFactory.getLogger(SimpleTraceStoreSearchResult.class);

    private SimpleTraceStore store;

    private TraceSearchQuery query;
    private QmiNode qmi;

    private StructuredTextIndex itext;

    private Fun.Tuple2<Long,Integer> cursor;
    private ConcurrentNavigableMap<Fun.Tuple2<Long,Integer>,Long> tstamps;

    public SimpleTraceStoreSearchResult(TraceSearchQuery query, SimpleTraceStore store) {
        this.query = query;
        this.store = store;
        this.itext = store.getTextIndex();

        this.qmi =  query.getQmi();

        if (qmi == null) {
            qmi = new QmiNode();
        }

        int limit = Integer.max(256, query.getLimit() * 4 + query.getOffset());

        this.tstamps = store.getTstamps();
        this.cursor = this.tstamps.size() > 0 ? this.tstamps.lastKey() : null;

        for (int i = 0; cursor != null && i < query.getOffset(); i++) {
            cursor = tstamps.lowerKey(cursor);
        }
    }

    private TraceSearchResultItem extractChunkMetadata(int slot) {
        long chunkId = store.toChunkId(slot);
        ChunkMetadata md = store.getChunkMetadata(chunkId);

        TraceSearchResultItem itm = new TraceSearchResultItem(chunkId, md);

        itm.setChunkId(chunkId);
        itm.setDescription(store.getDesc(chunkId));
        itm.setTraceId1(md.getTraceId1());
        itm.setTraceId2(md.getTraceId2());
        itm.setParentId(md.getParentId());
        itm.setSpanId(md.getSpanId());

        return itm;
    }

    @Override
    public TraceSearchResultItem nextItem() {

        if (cursor != null) {
            int seq = cursor.b;
            cursor = tstamps.lowerKey(cursor);
            return extractChunkMetadata(seq);
        }

        return null;
    }

    @Override
    public int size() {
        return tstamps.size();
    }

}
