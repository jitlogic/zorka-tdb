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

import io.zorka.tdb.search.SearchNode;
import io.zorka.tdb.search.rslt.SearchResult;
import io.zorka.tdb.text.AbstractTextIndex;
import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.util.ZicoUtil;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class TextIndexR extends AbstractTextIndex implements TextIndex {

    protected final NavigableMap<Integer,String> words = new TreeMap<>();
    protected int idBase, nWords;
    protected long dataLen;

    public TextIndexR(int idBase, int nWords, long dataLen) {
        this.idBase = idBase;
        this.nWords = nWords;
        this.dataLen = dataLen;
    }

    @Override
    public String getPath() {
        return String.format("test-%08x.ifm", idBase);
    }

    @Override
    public int getIdBase() {
        return idBase;
    }

    @Override
    public int getNWords() {
        synchronized (words) {
            return nWords >= 0 ? nWords : words.size();
        }
    }

    @Override
    public long getDatalen() {
        return dataLen;
    }

    @Override
    public byte[] get(int id) {
        synchronized (words) {
            String s = words.get(id);
            return s != null ? s.getBytes() : null;
        }
    }

    @Override
    public int get(byte[] buf, int offs, int len, boolean esc) {
        String s = new String(buf, offs, len, Charset.forName("utf8"));
        synchronized (words) {
            for (Map.Entry<Integer, String> e : words.entrySet()) {
                if (s.equals(e.getValue())) {
                    return e.getKey();
                }
            }
        }
        return -1;
    }

    @Override
    public long length() {
        return dataLen;
    }

    @Override
    public SearchResult searchIds(long tid, boolean deep) {
        return null;
    }

    @Override
    public void close() { }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TextIndexR) {
            TextIndexR x = (TextIndexR)obj;
            return idBase == x.idBase
                    && nWords == x.nWords && dataLen == x.dataLen
                    && ZicoUtil.objEquals(words, x.words);
        }
        return false;
    }

    @Override
    public String toString() {
        return "R(" + idBase + "," + nWords + "," + dataLen + ")";
    }

    public NavigableMap<Integer,String> getWords() {
        return words;
    }

    @Override
    public SearchResult search(SearchNode expr) {
        return null;
    }

}
