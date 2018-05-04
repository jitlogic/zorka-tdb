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

import io.zorka.tdb.text.RawDictCodec;
import io.zorka.tdb.text.RawDictCodec;

/**
 *
 */
public interface SearchBufView {

    int position();

    void position(int pos);

    int nextChar();

    boolean drained();

    default boolean nextTerm() {
        int ch;

        do {
            ch = nextChar();
        } while (ch != -1 && ch != RawDictCodec.MARK_TXT);

        return ch != -1;
    }

}
