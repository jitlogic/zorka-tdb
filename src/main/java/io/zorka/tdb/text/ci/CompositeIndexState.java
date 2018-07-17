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

package io.zorka.tdb.text.ci;

import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.text.WritableTextIndex;

import java.util.Collections;
import java.util.List;

/**
 *
 */
public class CompositeIndexState {

    private final List<TextIndex> allIndexes;
    private final List<TextIndex> lookupIndexes;
    private final List<TextIndex> searchIndexes;
    private final WritableTextIndex currentIndex;

    public CompositeIndexState(
            List<TextIndex> allIndexes,
            List<TextIndex> lookupIndexes,
            List<TextIndex> searchIndexes,
            boolean archived) {
        this.allIndexes = Collections.unmodifiableList(allIndexes);
        this.lookupIndexes = Collections.unmodifiableList(lookupIndexes);
        this.searchIndexes = Collections.unmodifiableList(searchIndexes);
        // TODO wyszukiwanie bieżącego indeksu przneieść poziom wyżej (a tutaj jako parametr konstruktora)
        if (!archived && lookupIndexes.size() > 0) {
            TextIndex index = lookupIndexes.get(0);
            currentIndex = index.isWritable() ? (WritableTextIndex) index : null;
        } else {
            currentIndex = null;
        }

    }

    public WritableTextIndex getCurrentIndex() {
        return currentIndex;
    }

    public List<TextIndex> getAllIndexes() { return allIndexes; }

    public List<TextIndex> getLookupIndexes() {
        return lookupIndexes;
    }

    public List<TextIndex> getSearchIndexes() {
        return searchIndexes;
    }

    @Override
    public String toString() {
        return "CompositeIndexState(current=" + currentIndex
                + ", search=" + searchIndexes
                + ", lookup=" + lookupIndexes
                + ", all=" + allIndexes + ")";
    }
}
