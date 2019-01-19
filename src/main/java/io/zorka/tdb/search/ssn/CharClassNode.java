/*
 * Copyright 2016-2019 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
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

package io.zorka.tdb.search.ssn;

import java.util.BitSet;

/**
 * Matches by character class. This is intended to be used in bigger logical constructs
 * as searching for single characters across vast stores does not make sense.
 * Note that this works only on bytes, thus character classes are valid only for ASCII characters.
 */
public class CharClassNode implements StringSearchNode {

    private BitSet chars = new BitSet();

    public void addRange(char first, char last) {
        addRange((int)first, (int)last);
    }

    public void addRange(int first, int last) {
        chars.set(first, last+1);
    }

    public BitSet getChars() {
        return chars;
    }

    @Override
    public String toString() {
        return "CharClassNode(" + chars + ")";
    }
}

