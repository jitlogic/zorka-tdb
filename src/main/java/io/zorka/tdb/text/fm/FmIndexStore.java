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

package io.zorka.tdb.text.fm;

import java.io.Closeable;

public interface FmIndexStore extends Closeable {


    /**
     * Returns character and its rank.
     * @param pos logical position
     * @return character and its rank; use chr() and rnk() to extract char or rank
     */
    long charAndRank(int pos);


    /**
     * Returns rank of specific character at specific position.
     * @param pos logical position (as in uncompressed BWT string)
     * @param chr character
     * @return rank
     */
    int rankOf(int pos, byte chr);


    /**
     * Return position of first character in sorted text (L column).
     * @param ch character
     * @return logical character position (as in uncompressed BWT string)
     */
    int getCharOffs(byte ch);


    /**
     * Returns number of blocks in this store.
     * @return number of blocks
     */
    long getNBlocks();


    /**
     * Returns (logical) length of uncompressed BWT string stored.
     * @return logical length
     */
    int getDatalen();


    /**
     * Returns ID of first ID-text pair stored.
     * @return ID
     */
    int getIdBase();


    /**
     * Returns number of ID-text pairs stored.
     * @return number of ID-text pairs stored
     */
    int getNWords();


    int getPidx();


    int getData(byte[] buf, int lpos);

    /**
     * Returns physical file length.
     */
    long length();

    /**
     * Encodes character and its rank into one 64-bit integer.
     * @param ch character value (byte)
     * @param rnk character rank (integer)
     * @return char-and-rank pair
     */
    static long car(byte ch, int rnk) { return (((long)rnk) << 8) | (ch & 0xffL); }


    /**
     * Extracts character from char-and-rank pair.
     * @param car char-and-rank pair
     * @return character component of char-and-rank pair
     */
    static byte chr(long car) {
        return (byte)(car & 0xffL);
    }

    static int chi(long car) {
        return (int)(car & 0xffL);
    }


    /**
     * Extracts character rank component from char-and-rank pair.
     * @param car char-and-rank pair
     * @return character rank compoent of char-and-rank pair
     */
    static int rnk(long car) {
        return (int)(car >>> 8);
    }

}
