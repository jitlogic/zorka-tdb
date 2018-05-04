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

package io.zorka.tdb.test.unit.text.wal;

import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.fm.FmTextIndex;

import io.zorka.tdb.text.wal.WalTextIndex;

import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.fm.FmTextIndex;
import io.zorka.tdb.text.wal.WalTextIndex;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 *
 */
public class WalPatternSearchUnitTest extends ZicoTestFixture {

    private final static String[] TERMS = {
        "ABRACADABRA",  // 0
        "BROMABA",      // 1
        "ABABA"         // 2
    };

    private static WalTextIndex widx;
    private static FmTextIndex fidx;

    @BeforeClass
    static void createIndexes() throws Exception {
        widx = createWalTextIndex("/tmp/test.wal", TERMS);
        fidx = createFmTextIndex("/tmp/test.fmi", TERMS);
    }

    @AfterClass
    static void closeIndexes() throws Exception {
        widx.close();
        fidx.close();
    }

//    private void check(IndexType type, String regex, Integer...rslts) {
//        TextIndex idx = type == IndexType.FMI ? fidx : widx;
//        assertAll("Matching /" + regex + "/",
//            () -> assertEquals(set(rslts), idx.searchIds(regex).toSet()));
//    }
//
//    @ParameterizedTest @EnumSource(IndexType.class)
//    void testSearchSimpleText(IndexType type) {
//        check(type, "BA", 1, 2);
//    }
//
//
//    @ParameterizedTest @EnumSource(IndexType.class)
//    void testBeginAndEndMarkers(IndexType type) {
//        check(type, "^AB", 0, 2);
//        check(type, "RA$", 0);
//    }
//
//
//    @ParameterizedTest @EnumSource(IndexType.class)
//    void testMaskedRegex(IndexType type) {
//        check(type, ".*OM.*BA", 1);
//        check(type, "^BR.*BA", 1);
//    }
//
//    @ParameterizedTest @EnumSource(IndexType.class)
//    void testMaskedRegex2(IndexType type) {
//        check(type, ".*OM.*BA$", 1);
//    }
}
