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

package io.zorka.tdb.test.unit.util;

import io.zorka.tdb.test.support.TestUtil;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;

import java.util.Random;

import static io.zorka.tdb.text.TextIndexUtils.*;
import static org.junit.Assert.*;

public class TextIndexUtilsUnitTest {


    @Test
    public void testStringEscapeFn() {
        byte[] buf = DatatypeConverter.parseBase64Binary("UGFyX1/ClcK+c3IAJG9yZy5qYm9zcy5pbnZvY2F0aW9uLk1hcnNoYWxsZWRWYWx1ZcOqw4zDoMORw7RKw5DCmQwAAHhwegAAASEAAAEZwqzDrQAFdXIAE1tMamF2YS5sYW5nLk9iamVjdDvCkMOOWMKfEHMpbAIAAHhwAAAABHNyABtqYXZheC5tYW5hZ2VtZW50Lk9iamVjdE5hbWUPA8KnG8OrbRXDjwMAAHhwdAAsamJvc3MuYWRtaW46c2VydmljZQ==");
        byte[] out = escape(buf, 0, buf.length, LIM_WAL);

        assertNotNull(out);

        for (int i = 0; i < out.length; i++) {
            if (out[i] == ESC) {
                i++;
            } else {
                assertFalse(out[i] >= ESC && out[i] < LIM_WAL);
            }
        }

        byte[] dec = unescape(out);

        TestUtil.assertEquals("Encoded and decoded don't match.", buf, dec);
    }

    @Test
    public void testStringEscapeEscChar() {
        byte[] buf = { 65, -1, 66, 0, 67, 1, 68, 2, 69, 3, 70, 4, 71, 5 };
        byte[] enc = escape(buf);
        byte[] dec = unescape(enc);

        for (int i = 0; i < enc.length; i++) {
            byte b = enc[i];
            if (b >= 0 && b < LIM_WAL) {
                fail("Invalid character at position: " + i + ": " + b);
                TestUtil.printBytes("enc", enc);
            }
        }

        //TestUtil.printBytes("buf", buf);
        //TestUtil.printBytes("enc", enc);
        //TestUtil.printBytes("dec", dec);

        TestUtil.assertEquals("buf != dec", buf, dec);
    }



    @Test
    public void testRandomStringEncDec() {
        byte[] buf = new byte[256];
        Random rand = new Random();
        for (int i = 0; i < 1000; i++) {
            rand.nextBytes(buf);
            byte[] enc = escape(buf);
            assertNotNull(enc);
            byte[] dec = unescape(enc);

            TestUtil.assertEquals("buf != dec", buf, dec);
        }
    }

}
