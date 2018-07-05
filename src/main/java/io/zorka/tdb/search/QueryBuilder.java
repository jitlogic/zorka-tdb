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

import io.zorka.tdb.ZicoException;
import io.zorka.tdb.search.lsn.AndExprNode;
import io.zorka.tdb.search.lsn.OrExprNode;
import io.zorka.tdb.search.lsn.SeqExprNode;
import io.zorka.tdb.search.ssn.CharClassNode;
import io.zorka.tdb.search.ssn.StringSearchNode;
import io.zorka.tdb.search.ssn.TextNode;
import io.zorka.tdb.search.tsn.KeyValSearchNode;

import java.util.ArrayList;
import java.util.List;

public class QueryBuilder {

    protected SearchNode node;
    private int limit = Integer.MAX_VALUE, offset = 0, window = 1048576;
    private long after = -1L;
    private SortOrder sortOrder = SortOrder.NONE;
    private boolean sortReverse = false, deepSearch = true;

    public static QueryBuilder stext(String s) {
        return text(s.getBytes(), false, false);
    }

    public static QueryBuilder xtext(String s) {
        return text(s.getBytes(), true, true);
    }

    public static QueryBuilder text(String s, boolean matchStart, boolean matchEnd) {
        return text(s.getBytes(), matchStart, matchEnd);
    }

    public static QueryBuilder stext(byte[] s) {
        return text(s, false, false);
    }

    public static QueryBuilder xtext(byte[] s) {
        return text(s, true, true);
    }

    public static QueryBuilder text(byte[] s, boolean matchStart, boolean matchEnd) {
        return new QueryBuilder(new TextNode(s, matchStart, matchEnd));
    }

    public static QueryBuilder charClass(int...ranges) {
        CharClassNode n = new CharClassNode();
        for (int i = 1; i < ranges.length; i += 2) {
            n.addRange(ranges[i-1], ranges[i]);
        }
        return new QueryBuilder(n);
    }

    public static QueryBuilder charClass(char...ranges) {
        CharClassNode n = new CharClassNode();
        for (int i = 1; i < ranges.length; i += 2) {
            n.addRange(ranges[i-1], ranges[i]);
        }
        return new QueryBuilder(n);
    }

    private static List<SearchNode> toNodes(Object...args) {
        List<SearchNode> rslt = new ArrayList<>(args.length);

        for (Object arg : args) {
            if (arg instanceof SearchNode) {
                rslt.add((SearchNode)arg);
            } else if (arg instanceof QueryBuilder) {
                rslt.add(((QueryBuilder)arg).node);
            } else if (arg instanceof TraceSearchQuery) {
                rslt.add(((TraceSearchQuery)arg).getNode());
            } else if (arg instanceof String) {
                rslt.add(new TextNode(((String)arg).getBytes(), false, false));
            } else {
                throw new ZicoException("Illegal argument: " + arg);
            }
        }

        return rslt;
    }

    public static QueryBuilder and(Object...args) {
        return new QueryBuilder(new AndExprNode(toNodes(args)));
    }

    public static QueryBuilder or(Object...args) {
        return new QueryBuilder(new OrExprNode(toNodes(args)));
    }

    public static QueryBuilder seq(boolean continous, Object...args) {
        return new QueryBuilder(new SeqExprNode(continous, toNodes(args)));
    }

    public static QmiQueryBuilder qmi() {
        return new QmiQueryBuilder();
    }

    public static QueryBuilder kv(String k, String v) {
        return new QueryBuilder(new KeyValSearchNode(k, TextNode.exact(v)));
    }

    public static QueryBuilder kv(String k, StringSearchNode n) {
        return new QueryBuilder(new KeyValSearchNode(k, n));
    }

    QueryBuilder(SearchNode node) {
        this.node = node;
    }

    public QueryBuilder limit(int n) {
        this.limit = n;
        return this;
    }

    public QueryBuilder offset(int n) {
        this.offset = n;
        return this;
    }

    public QueryBuilder after(long pos) {
        this.after = pos;
        return this;
    }

    public QueryBuilder window(int size) {
        this.window = size;
        return this;
    }

    public QueryBuilder deep() {
        this.deepSearch = true;
        return this;
    }

    public QueryBuilder shallow() {
        this.deepSearch = false;
        return this;
    }

    public QueryBuilder sort(SortOrder order) {
        return sort(order, false);
    }

    public QueryBuilder sort(SortOrder order, boolean reverse) {
        this.sortOrder= order;
        this.sortReverse = reverse;
        return this;
    }

    public TraceSearchQuery query() {
        return query(new QmiNode());
    }

    /** Return built search query */
    public TraceSearchQuery query(QmiNode qmi) {
        TraceSearchQuery q = new TraceSearchQuery(qmi, node);

        q.setLimit(limit);
        q.setOffset(offset);
        q.setAfter(after);
        q.setWindow(window);
        q.setSortOrder(sortOrder);
        q.setSortReverse(sortReverse);
        q.setDeepSearch(deepSearch);

        return q;
    }


    public SearchNode node() {
        return node;
    }
}
