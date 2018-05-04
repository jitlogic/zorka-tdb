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

import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.text.re.SearchPatternNode;
import io.zorka.tdb.util.IntegerSeqResult;

import java.io.Closeable;

/**
 * Read only full text index. Basic API.
 * It maps
 */
public interface TextIndex extends Closeable {

    String getPath();

    int getIdBase();

    default int getFirstId() {
        return getIdBase();
    }

    default int getLastId() {
        return getIdBase() + getNWords();
    }

    default boolean isWritable() {
        return this instanceof WritableTextIndex;
    }

    default boolean isReadOnly() {
        return !isWritable();
    }

    default boolean isOpen() {
        return TextIndexState.OPEN.equals(getState());
    }

    /**
     * Returns number of words in this index.
     */
    int getNWords();

    /**
     * Returns underlying data length.
     */
    long getDatalen();

    /**
     * Returns given term.
     * @param id term ID
     * @return retrieved term (as byte array)
     */
    byte[] get(int id);

    /**
     * Returns given term.
     * @param id term ID
     * @return retrieved term (as byte array)
     */
    default String gets(int id) {
        byte[] rslt = this.get(id);
        return rslt != null ? new String(rslt) : null;
    }

    /**
     * Looks for term and returns its ID
     * @param s term
     * @return term ID or -1 if term not indexed
     */
    default int get(String s)  {
        return get(s.getBytes());
    }

    /**
     * Returns term ID
     * @param buf term (as byte array)
     * @return term ID or -1 if term not indexed
     */
    default int get(byte[] buf)  {
        return get(buf, 0, buf.length, true);
    }

    default int get(byte[] buf, int offs, int len) {
        return get(buf, offs, len, true);
    }

    /**
     * Returns term ID.
     * @param buf byte array containing term
     * @param offs position in buf where term starts
     * @param len length of term (bytes)
     * @return term ID or -1 if term not indexed
     */
    int get(byte[] buf, int offs, int len, boolean esc);

    /**
     * Returns physical file length.
     */
    long length();


    IntegerSeqResult searchIds(SearchPatternNode node);


    /**
     * Searches index for terms matching given pattern. Returns iterable object generating term IDS.
     * @param pattern
     * @return
     */
    IntegerSeqResult searchIds(SearchPattern pattern);


    default IntegerSeqResult searchIds(String text) {
        return searchIds(new SearchPattern(text));
    }

    /**
     * Looks for given phrase, then extracts encoded integer immediately preceding phrase up to a specific marker byte
     * or beginning of indexed string. Search is performed backwards. This method performs much better than searching
     * for dictionary keys and extracting values in separate steps.
     * @param phrase search phrase (string bytes)
     * @param m1 marker starting preceeding value
     * @return iterable result
     */
    IntegerSeqResult searchXIB(byte[] phrase, byte m1);


    TextIndexState getState();

    void setState(TextIndexState state);

    boolean canRemove();

    void markForRemoval(long timeout);
}
