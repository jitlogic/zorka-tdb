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
 *
 */
public class AltPatternNode implements SearchPatternNode {

    private SearchPatternNode node1, node2;

    public AltPatternNode(SearchPatternNode node1, SearchPatternNode node2) {
        this.node1 = node1;
        this.node2 = node2;
    }

    public SearchPatternNode getNode1() {
        return node1;
    }

    public SearchPatternNode getNode2() {
        return node2;
    }

    public String toString() {
        return node1.toString() + "|" + node2.toString();
    }

    @Override
    public SearchPatternNode invert() {
        return new AltPatternNode(node2.invert(), node1.invert());
    }

    @Override
    public int match(SearchBufView view) {
        int rslt = node1.match(view);
        return rslt > 0 ? rslt : node2.match(view);
    }

    @Override
    public void visitMtns(SearchPatternNodeHandler handler) {
    }
}
