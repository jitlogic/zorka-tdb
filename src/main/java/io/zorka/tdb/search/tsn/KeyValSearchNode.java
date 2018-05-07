package io.zorka.tdb.search.tsn;

import io.zorka.tdb.search.ssn.StringSearchNode;

/**
 * Searches trace data for key-value pairs
 */
public class KeyValSearchNode implements TraceSearchNode {

    private String key;
    private StringSearchNode val;

    public KeyValSearchNode(String key, StringSearchNode val) {
        this.key = key;
        this.val = val;
    }

    public String getKey() {
        return key;
    }

    public StringSearchNode getVal() {
        return val;
    }
}
