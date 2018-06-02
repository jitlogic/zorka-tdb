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

public class TextIndexUtils {

    public final static byte ESC = -1;

    public static final byte LIM_WAL = 4;

    public static byte[] escape(byte[] buf) {
        return escape(buf, LIM_WAL);
    }

    public static byte[] escape(byte[] buf, byte lim) {
        return escape(buf, 0, buf.length, lim);
    }

    public static byte[] escape(byte[] buf, int offs, int len) {
        return escape(buf, offs, len, LIM_WAL);
    }

    public static byte[] escape(byte[] buf, int offs, int len, byte lim) {
        int cnt = 0;

        for (int i = 0; i < len; i++) {
            if (buf[i+offs] >= ESC && buf[i+offs] < lim) cnt++;
        }

        if (cnt == 0) return buf;

        byte[] rslt = new byte[len+cnt];

        int o = 0;
        for (int i = 0; i < len; i++) {
            byte b = buf[i+offs];
            if (b >= ESC && b < lim) {
                rslt[o++] = ESC;
                rslt[o++] = (byte)(b == ESC ? 8 : b+lim);
            } else {
                rslt[o++] = b;
            }
        }

        return rslt;
    }

    public static byte[] unescape(byte[] buf) {
        return unescape(buf, LIM_WAL);
    }

    public static byte[] unescape(byte[] buf, byte lim) {
        return unescape(buf, 0, buf.length, lim);
    }

    public static byte[] unescape(byte[] buf, int offs, int len) {
        return unescape(buf, offs, len, LIM_WAL);
    }

    public static byte[] unescape(byte[] buf, int offs, int len, byte lim) {
        int cnt = len;

        for (int i = 0; i < len; i++) {
            if (buf[offs+i] == ESC) {
                cnt--; i++;
            }
        }

        if (cnt == len) return buf;

        byte[] rslt = new byte[cnt];

        int o = 0;
        for (int i = 0; i < len; i++) {
            byte b = buf[i+offs];
            if (b == ESC) {
                i++;
                b = buf[i+offs];
                rslt[o++] = (byte)(b == 8 ? ESC : b - lim);
            } else {
                rslt[o++] = b;
            }
        }

        return rslt;
    }
}
