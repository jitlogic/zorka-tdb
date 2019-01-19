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

package io.zorka.tdb.util;

import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 *
 */
public class BitUtils {
    @SuppressWarnings("restriction")
    private static sun.misc.Unsafe getUnsafe() {
        try {

            Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (sun.misc.Unsafe) f.get(null);

        } catch (IllegalArgumentException | SecurityException | NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static final sun.misc.Unsafe UNSAFE = getUnsafe();

    public static final long BYTE_ARRAY_OFFS = UNSAFE.arrayBaseOffset(byte[].class);

    public static long addr(ByteBuffer buf) {
        return ((DirectBuffer)buf).address();
    }

    public final static boolean NET_ORDER;

    static {
        byte[] b = new byte[]{0x11, 0x22, 0x33, 0x44};

        int i1 = UNSAFE.getInt(b, BYTE_ARRAY_OFFS);
        int i2 = ByteBuffer.wrap(b).getInt();

        NET_ORDER =  i1 == i2;
    }

    public static short ntohs(short v) {
        return NET_ORDER ? v : (short)((v >>> 8) | (v << 8));
    }

    public static int ntohi(int v) {
        return NET_ORDER ? v :
                  ((v >>> 24)
                | ((v >>> 8)  & 0x0000_ff00)
                | ((v <<  8)  & 0x00ff_0000)
                |  (v << 24));
    }

    public static long ntohl(long v) {
        return NET_ORDER ? v :
                  ((v >>> 56)
                | ((v >>> 40) & 0x0000_0000_0000_ff00L)
                | ((v >>> 24) & 0x0000_0000_00ff_0000L)
                | ((v >>>  8) & 0x0000_0000_ff00_0000L)
                | ((v <<   8) & 0x0000_00ff_0000_0000L)
                | ((v <<  24) & 0x0000_ff00_0000_0000L)
                | ((v <<  40) & 0x00ff_0000_0000_0000L)
                |  (v <<  56));
    }


    public static void putInt48(ByteBuffer buf, int index, long v) {
        buf.putShort(index, (short)(v >>> 32));
        buf.putInt(index+2, (int)v);
    }


    public static long getInt48(ByteBuffer buf, int index) {
        return ((buf.getShort(index) & 0xffffL) << 32) | (buf.getInt(index+2) & 0xffffffffL);
    }


    public static int getInt8(long addr) {
        return UNSAFE.getByte(addr) & 0xff;
    }


    public static int getInt16(long addr) {
        return ((UNSAFE.getByte(addr) & 0xff) << 8)  |  (UNSAFE.getByte(addr+1) & 0xff);
    }


    public static int getInt32(long addr) {
        return ((UNSAFE.getByte(addr) & 0xff) << 24) | ((UNSAFE.getByte(addr+1) & 0xff) << 16)
             | ((UNSAFE.getByte(addr+2) & 0xff) << 8) | (UNSAFE.getByte(addr+3) & 0xff);
    }


    public static long getInt48(long addr) {
        return    ((UNSAFE.getByte(addr) & 0xffL) << 40) | ((UNSAFE.getByte(addr+1) & 0xffL) << 32)
                | ((UNSAFE.getByte(addr+2) & 0xffL) << 24) | ((UNSAFE.getByte(addr+3) & 0xffL) << 16)
                | ((UNSAFE.getByte(addr+4) & 0xffL) << 8)  |  (UNSAFE.getByte(addr+5) & 0xffL);
    }


    public static long getInt64(long addr) {
        return ((UNSAFE.getByte(addr) & 0xffL) << 56) | ((UNSAFE.getByte(addr+1) & 0xffL) << 48)
             | ((UNSAFE.getByte(addr+2) & 0xffL) << 40) | ((UNSAFE.getByte(addr+3) & 0xffL) << 32)
             | ((UNSAFE.getByte(addr+4) & 0xffL) << 24) | ((UNSAFE.getByte(addr+5) & 0xffL) << 16)
             | ((UNSAFE.getByte(addr+6) & 0xffL) << 8)  |  (UNSAFE.getByte(addr+7) & 0xffL);
    }
}
