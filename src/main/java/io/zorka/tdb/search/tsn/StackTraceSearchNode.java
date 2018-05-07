package io.zorka.tdb.search.tsn;

import java.util.List;

public class StackTraceSearchNode implements TraceSearchNode {

    private boolean matchStart, matchEnd;

    private List<StackElementSearchNode> stackTrace;

}
