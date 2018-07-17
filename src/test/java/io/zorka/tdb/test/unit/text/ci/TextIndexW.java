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

package io.zorka.tdb.test.unit.text.ci;

import io.zorka.tdb.text.WritableTextIndex;
import io.zorka.tdb.text.WritableTextIndex;

import java.nio.charset.Charset;

public class TextIndexW extends TextIndexR implements WritableTextIndex {

    private boolean forceRotate;

    public TextIndexW(int idBase, int nWords, long dataLen) {
        super(idBase, nWords, dataLen);
        forceRotate = false;
    }

    @Override
    public int add(byte[] buf, int offs, int len, boolean esc) {
        int id = get(buf, offs, len, esc);
        if (id != -1) return id;

        synchronized (this) {
            if (forceRotate) {
                return -1;
            }
        }

        synchronized (words) {
            id = idBase;
            if (words.size() > 0) {
                id = words.lastKey() + 1;
            }
            words.put(id, new String(buf, offs, len, Charset.forName("utf8")));
        }

        return id;
    }

    @Override
    public void flush() { }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TextIndexW) {
            TextIndexW x = (TextIndexW)obj;
            return idBase == x.idBase
                    && nWords == x.nWords
                    && dataLen == x.dataLen;
        }
        return false;
    }

    public void setForceRotate(boolean forceRotate) {
        this.forceRotate = forceRotate;
    }

    @Override
    public String toString() {
        return "W(" + (isOpen() ? "-" : "C" ) + (canRemove() ? "-" : "R" ) + ", " + idBase + "," + nWords + "," + dataLen + ")";
    }

    @Override
    public String getPath() {
        return String.format("test-%08x.wal", idBase);
    }
}
