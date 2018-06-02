/*
 * Copyright 2016-2017 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
 * <p/>
 * This is free software. You can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * <p/>
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * <p/>
 * You should have received a copy of the GNU General Public License
 * along with this software. If not, see <http://www.gnu.org/licenses/>.
 */

package io.zorka.tdb.search;

public class SearchQuery implements SearchNode {

    public static final SearchQuery DEFAULT = new SearchQuery(null);

    private SearchNode node;
    private int limit = Integer.MAX_VALUE, offset = 0, window = 1048576;
    private long after = -1;
    private boolean deepSearch = true;

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

    public boolean isDeepSearch() {
        return deepSearch;
    }

    public void setDeepSearch(boolean deepSearch) {
        this.deepSearch = deepSearch;
    }
}
