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

package io.zorka.tdb.text;

import io.zorka.tdb.ZicoException;

import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public class TextSearchEntryStrIterator implements Iterator<Map.Entry<Integer,String>> {

    private TextIndex idx;
    private Iterator<Integer> iter;

    public TextSearchEntryStrIterator(TextIndex idx, Iterable<Integer> src) {
        this.idx = idx;
        this.iter = src.iterator();
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public Map.Entry<Integer, String> next() {
        Integer id = iter.next();
        String str = idx.gets(id);
        return new Map.Entry<Integer,String>() {
            @Override
            public Integer getKey() {
                return id;
            }

            @Override
            public String getValue() {
                return str;
            }

            @Override
            public String setValue(String value) {
                throw new ZicoException("Search iterators are read only.");
            }
        };
    }
}
