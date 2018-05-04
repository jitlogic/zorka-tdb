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

package io.zorka.tdb.test.unit.util;

import io.zorka.tdb.util.BitUtils;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

import static io.zorka.tdb.util.BitUtils.*;

import java.nio.ByteBuffer;


/**
 *
 */
public class UnsafeUnitTest {

    @Test
    public void testNtohFunctions() throws Exception {
        byte[] buf = new byte[] { 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77 };
        ByteBuffer bb = ByteBuffer.wrap(buf);

        short s1 = BitUtils.UNSAFE.getShort(buf, BitUtils.BYTE_ARRAY_OFFS), s2 = bb.getShort(0);
        int i1 = BitUtils.UNSAFE.getInt(buf, BitUtils.BYTE_ARRAY_OFFS), i2 = bb.getInt(0);
        long l1 = BitUtils.UNSAFE.getLong(buf, BitUtils.BYTE_ARRAY_OFFS), l2 = bb.getLong(0);

        Assert.assertEquals(s2, BitUtils.ntohs(s1));
        Assert.assertEquals(i2, BitUtils.ntohi(i1));
        Assert.assertEquals(l2, BitUtils.ntohl(l1));
    }

}
