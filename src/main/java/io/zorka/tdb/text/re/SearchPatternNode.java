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
 * Common interface for all regex patern nodes.
 */
public interface SearchPatternNode {

    int ZERO_FAIL = Integer.MIN_VALUE;

    /**
     * Returns pattern node that will match the same strings backwards.
     */
    SearchPatternNode invert();

    /**
     * Attempts to match byte sequence represented by character view. If sequence matches pattern, method will return
     * positive number representing number of bytes consumed (or zero if pattern accepts zero-length matches). Otherwise
     * negative number will be returned (also representing number of character that would be consumed or Integer.MIN_VALUE
     * if no characters were consumed.
     *
     * @param view object containing matched byte sequence
     * @return match result (as described above)
     */
    int match(SearchBufView view);


    /**
     * Looks for all text nodes that must match in order to whole regex match.
     * @param handler handler that will be called for each text node found
     */
    void visitMtns(SearchPatternNodeHandler handler);
}
