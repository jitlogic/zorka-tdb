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
public class LoopPatternNode implements SearchPatternNode {

    private SearchPatternNode node;
    private int min, max;


    public LoopPatternNode(SearchPatternNode node, int min, int max) {
        this.node = node;
        this.min = min;
        this.max = max;
    }


    @Override
    public String toString() {
        return node.toString() + "{" + min + "," + max + "}";
    }


    @Override
    public SearchPatternNode invert() {
        return new LoopPatternNode(node.invert(), min, max);
    }


    @Override
    public int match(SearchBufView view) {
        return match(view, Integer.MAX_VALUE);
    }

    @Override
    public void visitMtns(SearchPatternNodeHandler handler) {
        if (min > 0) {
            node.visitMtns(handler);
        }
    }


    public int match(SearchBufView view, int limit) {
        int pos = view.position(), lim = Integer.min(max, limit), cnt, sum = 0, lsum = 0;

        for (cnt = 0; cnt < lim; cnt++) {
            lsum = node.match(view);
            if (lsum >= 0) {
                sum += lsum;
            } else {
                if (lsum == ZERO_FAIL) {
                    lsum = 0;
                }
                break;
            }
        }

        if (cnt < min) {
            view.position(pos);
            return (lsum - sum) != 0 ? (lsum - sum) : ZERO_FAIL;
        } else {
            return sum;
        }
    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }
}
