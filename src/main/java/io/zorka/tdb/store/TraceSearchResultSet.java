package io.zorka.tdb.store;

import java.util.*;

/**
 * Accumulates results of trace searches.
 */
public class TraceSearchResultSet {

    /** Limits number of items to be returned. */
    private int limit;

    /** Number of items to be skipped. */
    private int offset;

    /** If true, individual spans will be collected. If false, whole trace tops. */
    private boolean spansOnly;

    /** Visited results. */
    private Set<String> visited = new HashSet<>();

    /** Collected results. */
    private List<ChunkMetadata> results = new ArrayList<>();

    public TraceSearchResultSet(int offset, int limit, boolean spansOnly) {
        this.offset = offset;
        this.limit = limit;
        this.spansOnly = spansOnly;
    }

    public void add(ChunkMetadata cm) {
        String key = cm.getTraceIdHex() + (spansOnly ? cm.getSpanIdHex() : "");
        if (!visited.contains(key)) {
            if (visited.size() >= offset) results.add(cm);
            visited.add(key);
        }
    }

    /** Returns true if more results are needed. */
    public boolean needMore() {
        return visited.size() < limit+offset;
    }

    public int size() {
        return results.size();
    }

    public List<ChunkMetadata> getResults() {
        return Collections.unmodifiableList(results);
    }
}
