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

package io.zorka.tdb.test.manual;

import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.test.support.ZicoTestFixture;

import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.test.support.ZicoTestFixture;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

/**
 *
 */
public class SiftBwtDataManualTest {

    @Test
    public void createTestBwtInputFile() throws Exception {
        byte[] ibuf = TestUtil.readf(ZicoTestFixture.T300M_BWT, 32 * 1024 * 1024, false);
        byte[] obuf = new byte[65536];

        assertNotNull(ibuf);

        int ip = 1 * 1024 * 1024;
        byte lc = 0;
        int ln = 0;

        for (int op = 0; op < obuf.length; op++) {
            byte c = ibuf[ip++];
            if (c == lc) {
                if (ln < 128) {
                    ln++;
                    obuf[op] = c;
                } else {
                    //System.out.println("Break: ");
                    while (lc == (c = ibuf[ip])) {
                        ip++;
                    }
                    obuf[op] = c;
                    lc = 0;
                }
            } else {
                obuf[op] = c;
                lc = c;
            }
        }

        TestUtil.writef("/tmp/test.bwt", obuf);
    }

}
