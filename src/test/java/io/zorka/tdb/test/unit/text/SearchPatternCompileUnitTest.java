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

import io.zorka.tdb.text.re.SearchPattern;

import io.zorka.tdb.text.re.SearchPattern;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 */
public class SearchPatternCompileUnitTest {

    private String re(String pattern) {
        return new SearchPattern(pattern).getRoot().toString();
    }

    private String ri(String pattern) {
        return new SearchPattern(pattern).getRoot().invert().toString();
    }

    @Test
    public void testCompileRegexPattern() {
        assertEquals("'abc'", re("abc"));
        assertEquals("'a'{0,2147483647}", re("a*"));
        assertEquals("'ab'&'c'{1,2147483647}", re("abc+"));
        assertEquals("'abc'|'def'", re("abc|def"));
        assertEquals(".{0,2147483647}", re(".*"));
        assertEquals("[ab]{0,2147483647}", re("[ab]*"));
        assertEquals("'a'{2,2}", re("a{2}"));
        assertEquals("'a'{2,4}", re("a{2,4}"));
        assertEquals("('ab')&('cd')", re("(ab)(cd)"));
        assertEquals("'a'{0,1}&'b'{0,1}", re("a?b?"));
        assertEquals("[123abcd]", re("[a-d1-3]"));
        assertEquals("[0123456789]", re("\\d"));
        assertEquals("[^0123456789]", re("\\D"));
        assertEquals("[\t\n\r ]", re("\\s"));
        assertEquals("[^\t\n\r ]", re("\\S"));
        assertEquals("[^ab]", re("[^ab]"));
    }

    @Test
    public void testCompileInvertedPattern() {
        assertEquals("'cba'", ri("abc"));
        assertEquals("'a'{0,2147483647}", ri("a*"));
        assertEquals("'c'{1,2147483647}&'ba'", ri("abc+"));
        assertEquals("'fed'|'cba'", ri("abc|def"));
        assertEquals(".{0,2147483647}", ri(".*"));
        assertEquals("[ab]{0,2147483647}", ri("[ab]*"));
        assertEquals("'a'{2,2}", ri("a{2}"));
        assertEquals("'a'{2,4}", ri("a{2,4}"));
        assertEquals("('dc')&('ba')", ri("(ab)(cd)"));
        assertEquals("'b'{0,1}&'a'{0,1}", ri("a?b?"));
        assertEquals("'b'{0,1}|'a'{0,1}", ri("a?|b?"));
        assertEquals("[123abcd]", ri("[a-d1-3]"));
        assertEquals("[0123456789]", ri("\\d"));
        assertEquals("[^0123456789]", ri("\\D"));
        assertEquals("[\t\n\r ]", ri("\\s"));
        assertEquals("[^\t\n\r ]", ri("\\S"));
        assertEquals("[^ab]", ri("[^ab]"));
    }

}
