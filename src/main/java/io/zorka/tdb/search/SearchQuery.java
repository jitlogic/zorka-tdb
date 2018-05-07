package io.zorka.tdb.search;

public class SearchQuery {

    private SearchNode node;
    private int limit = Integer.MAX_VALUE, offset = 0, window = 1048576;
    private long after = -1;

    private SortOrder sortOrder = SortOrder.NONE;
    private boolean sortReverse = false;

    public SearchQuery(SearchNode node) {
        this.node = node;
    }

    public SearchNode getNode() {
        return node;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public long getAfter() {
        return after;
    }

    public void setAfter(long after) {
        this.after = after;
    }

    public int getWindow() {
        return window;
    }

    public void setWindow(int window) {
        this.window = window;
    }

    public SortOrder getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(SortOrder sortOrder) {
        this.sortOrder = sortOrder;
    }

    public boolean isSortReverse() {
        return sortReverse;
    }

    public void setSortReverse(boolean sortReverse) {
        this.sortReverse = sortReverse;
    }
}
