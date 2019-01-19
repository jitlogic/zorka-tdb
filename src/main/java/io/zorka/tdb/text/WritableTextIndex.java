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

package io.zorka.tdb.text;

/**
 *
 */
public interface WritableTextIndex extends TextIndex {
    default int add(String term)  {
        return add(term.getBytes());
    }

    default int add(byte[] buf)  {
        return add(buf, 0, buf.length, true);
    }

    default int add(byte[] buf, int offs, int len){
        return add(buf, offs, len, true);
    }

    int add(byte[] buf, int offs, int len, boolean esc);

    void flush();
}
