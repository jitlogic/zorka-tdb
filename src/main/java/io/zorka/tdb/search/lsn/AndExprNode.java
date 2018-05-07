package io.zorka.tdb.search.lsn;

import io.zorka.tdb.search.SearchNode;

import java.util.Collections;
import java.util.List;

public class AndExprNode implements LogicalExprNode {

    private List<SearchNode> args;

    public AndExprNode(List<SearchNode> args) {
        this.args = Collections.unmodifiableList(args);
    }

    public List<SearchNode> getArgs() {
        return args;
    }
}
