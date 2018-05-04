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

package io.zorka.tdb.text.re;

/**
 * Conjunction node (AND operator).
 */
public class ConjPatternNode implements SearchPatternNode {

    private SearchPatternNode node1, node2;

    public ConjPatternNode(SearchPatternNode node1, SearchPatternNode node2) {
        this.node1 = node1;
        this.node2 = node2;
    }

    public SearchPatternNode getNode1() {
        return node1;
    }

    public SearchPatternNode getNode2() {
        return node2;
    }

    @Override
    public String toString() {
        return node1.toString() + "&" + node2.toString();
    }

    @Override
    public SearchPatternNode invert() {
        return new ConjPatternNode(node2.invert(), node1.invert());
    }

    @Override
    public int match(SearchBufView view) {
        int pos = view.position();
        if (node1 instanceof LoopPatternNode) {
            LoopPatternNode ln1 = (LoopPatternNode)node1;
            int o1 = 0, o2 = 0;
            for (int i = ln1.getMin(); i < ln1.getMax(); i++) {
                int l1 = ln1.match(view, i);
                if (l1 < 0) {
                    view.position(pos);
                    return l1;
                }
                int l2 = node2.match(view);
                if (l2 >= 0) {
                    return l1 + l2;
                }
                view.position(pos);
                o2 = l2 != ZERO_FAIL ? l2 : 0;
                if (l1 != o1) {
                    o1 = l1;
                } else if (i > 0) {
                    return -l1;
                }
            }
            return o2 - o1;
        } else {
            int l1 = node1.match(view);
            if (l1 < 0) {
                return l1;
            }
            int l2 = node2.match(view);
            if (l2 < 0) {
                view.position(pos);
                return (l2 != ZERO_FAIL ? l2 : 0) - l1;
            }
            return l1 + l2;
        }
    }

    @Override
    public void visitMtns(SearchPatternNodeHandler handler) {
        node1.visitMtns(handler);
        node2.visitMtns(handler);
    }
}
