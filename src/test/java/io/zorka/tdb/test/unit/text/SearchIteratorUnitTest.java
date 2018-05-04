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

package io.zorka.tdb.test.unit.text;

import io.zorka.tdb.text.re.*;

import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.text.re.SearchPatternNode;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 */
public class SearchIteratorUnitTest {

    private void testRegex(String regex, String text, int result) {
        testRegex(regex, text, result, true);
    }

    private void testRegex(String regex, String text, int result, boolean checkConsumed) {
        StringBufView vnew = new StringBufView(text.getBytes());
        SearchPatternNode node = new SearchPattern(regex).getInverted();
        int rslt = node.match(vnew);
        assertEquals("RSLT1 '" + regex + "' ~ '" + text + "')'", result, rslt);
        if (checkConsumed)
            assertEquals("RSLT2 '" + regex + "' ~ '" + text + "')'", vnew.consumed(), rslt > 0 ? rslt : 0);

        // TODO check for position vs match result mismatch
    }


    @Test
    public void testLinearSearches() {
        testRegex("AB", "AB", 2);
        testRegex("AB", "xxxAB", 2);
        //testRegex("AB", "ABC", SeqPatternNode.ZERO_FAIL);
        testRegex("AB.", "ABC", 3);
        //testRegex("AB.", "AAC", -1);
        //testRe gex("BC", "ABC", 2);
    }


    @Test
    public void testLoopedSearches() {
        //testRegex("AB.*", "ABC", 3);
        //testRegex(".*AB.*", "xxxABxxx", 8);

        testRegex("AB.*CD", "ABCD", 4);
        //testRegex("AB.*CD", "ABxxCD", 6);
        testRegex("AB.*CD", "ABxxxD", -1);
        testRegex("AB.*CD", "xBxxCD", -6);

        //testRegex(".*A{2,3}.*", "xxAAAxx", 7);
        testRegex(".*A{2,3}.*", "xxAxx", -5);

        testRegex("\\d{3}", "123", 3);
        testRegex("\\d*A{2,3}", "123AAA", 2);
        //testRegex("\\d*A{2,3}\\d*", "123AAA123", 5);

        testRegex("\\d+A{2,3}\\d+", "123AAAA123", -3);
    }

    private int checkStringMatch(String s, String p) {
        StringBufView sb = new StringBufView(s, true);
        SearchPattern sp = SearchPattern.search(p);
        return sp.getRoot().match(sb);
    }

    @Test
    public void testPartialSeqNodeMatch() {
        assertTrue(checkStringMatch("XYZ", "XYZ") > 0);
        assertTrue(checkStringMatch("XYZ", "XY") > 0);
        assertTrue(checkStringMatch("XYZ", "YZ") > 0);
        assertTrue(checkStringMatch("XYZ", "XZ") < 0);
    }
}
