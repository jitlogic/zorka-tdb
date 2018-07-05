/*
 * Copyright 2016-2018 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
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

public class TraceSearchQuery implements SearchNode {

    public static final TraceSearchQuery DEFAULT = new TraceSearchQuery(new QmiNode(), null);

    // TODO uwzględnić w searchu flagi

    /** Fetch description text */
    public static final int FETCH_DESC = 0x0001;

    /** Fetch trace UUIDs */
    public static final int FETCH_UUID = 0x0002;

    /** Chunk-level search, do not */
    public static final int FETCH_CHNK = 0x0004;

    public static final int FETCH_ALL = 0xffff;

    /**
     * Determines which parts of results should be returned.
     */
    private int resultFlags = FETCH_ALL;

    /**
     * Optimization. Block size for QMI index scans.
     */
    private int blkSize = 16384;

    private SearchNode node;
    private QmiNode qmi;
    private int limit = Integer.MAX_VALUE, offset = 0, window = 1024;
    private long after = -1;
    private boolean deepSearch = true;

    private SortOrder sortOrder = SortOrder.NONE;
    private boolean sortReverse = false;

    public TraceSearchQuery(QmiNode qmi, SearchNode node) {
        this.qmi = qmi;
        this.node = node;
    }

    public QmiNode getQmi() {
        return qmi;
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

    public int getBlkSize() {
        return blkSize;
    }

    public void setBlkSize(int blkSize) {
        this.blkSize = blkSize;
    }
}
