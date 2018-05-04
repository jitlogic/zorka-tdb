/**
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

package io.zorka.tdb.meta;

import io.zorka.tdb.text.RawDictCodec;

/**
 *
 */
public class MetaIndexUtils {

    public static byte[] encodeMetaInt(byte m1, int v, byte m2) {
        byte[] rslt = new byte[RawDictCodec.idLen(v)+2];
        rslt[0] = m1;
        RawDictCodec.idEncode(rslt, 1, v);
        rslt[rslt.length-1] = m2;
        return rslt;
    }

    public static byte[] encodeMetaStr(byte m1, String s, byte m2) {
        byte[] b = s.getBytes(), rslt = new byte[2 + b.length];
        rslt[0] = m1;
        System.arraycopy(b, 0, rslt, 1, b.length);
        rslt[rslt.length-1] = m2;
        return rslt;
    }

    public static int getInt(byte[] buf, byte sep, int idx) {
        int i1 = pos1(buf, sep, idx), i2 = pos2(buf, sep, i1);

        if (i1 == buf.length) {
            return -1;
        }

        return (int)RawDictCodec.idDecode(buf, i1, i2-i1);

    }

    public static int countMarkers(byte[] buf, byte marker) {
        int rslt = 0;

        for (byte b : buf) {
            if (b == marker) {
                rslt++;
            }
        }

        return rslt;
    }

    public static String getStr(byte[] buf, byte sep, int idx) {
        int i1 = pos1(buf, sep, idx), i2 = pos2(buf, sep, i1);

        if (i1 == buf.length) {
            return null;
        }

        byte[] rslt = new byte[i2-i1];
        System.arraycopy(buf, i1, rslt, 0, i2-i1);
        return new String(rslt);
    }

    private static int pos2(byte[] buf, byte sep, int i1) {
        int i2;
        for (i2 = i1; i2 < buf.length; i2++) {
            if (buf[i2] == sep) {
                break;
            }
        }
        return i2;
    }

    private static int pos1(byte[] buf, byte sep, int idx) {
        int i1;
        for (i1 = 0; i1 < buf.length; i1++) {
            if (buf[i1] == sep) {
                if (idx == 0) {
                    i1++;
                    break;
                } else {
                    idx--;
                }
            }
        }
        return i1;
    }

}
