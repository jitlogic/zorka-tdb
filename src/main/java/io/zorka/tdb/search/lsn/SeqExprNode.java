package io.zorka.tdb.search.lsn;

import io.zorka.tdb.search.SearchNode;

import java.util.Collections;
import java.util.List;

public class SeqExprNode implements LogicalExprNode {

    private boolean continous;

    private List<SearchNode> args;

    public SeqExprNode(boolean continous, List<SearchNode> args) {
        this.continous = continous;
        this.args = Collections.unmodifiableList(args);
    }

    public boolean isContinous() {
        return continous;
    }

    public List<SearchNode> getArgs() {
        return args;
    }

}
