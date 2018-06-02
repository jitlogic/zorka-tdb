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

package io.zorka.tdb.test.unit.text.fm;

import io.zorka.tdb.text.fm.BWT;
import org.junit.Test;

import java.nio.charset.Charset;

import static org.junit.Assert.*;

public class BWTEncodingUnitTest {

    @Test
    public void testEncodeDecodeBwtString() {
        String s0 = "- And who is Zed ?\n- Zed's dead baby, Zed's dead.";
        byte[] bwt = new byte[s0.getBytes().length];
        int pidx = BWT.bwtencode(s0.getBytes(), bwt);
        byte[] txt = BWT.bwtdecode(bwt, pidx);
        String s1 = new String(txt, Charset.forName("utf8"));
        assertEquals(s0, s1);
    }
}
