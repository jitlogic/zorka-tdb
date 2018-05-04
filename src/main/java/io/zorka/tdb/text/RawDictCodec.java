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

import io.zorka.tdb.ZicoException;

/**
 * Basic functions for ID and term encoding.
 */
public class RawDictCodec {

    /** ID marker. Occurs just before leading ID (that is, ID placed before text). */
    public static final byte MARK_ID1   = 0x01;

    /** Text marker. */
    public static final byte MARK_TXT   = 0x02;

    public static final byte MARK_ID2   = 0x03;

    public static final byte MARK_LAST  = MARK_ID2;

    public final static int ENC_OFF = 48;

    private final static int ENC_BITS = 6;
    private final static int ENC_MAX = 1 << ENC_BITS;
    private final static int ENC_MASK = ENC_MAX - 1;


    private final static long ENC_D1 = ENC_MAX;
    private final static long ENC_D2 = ENC_MAX * ENC_D1;  // 4096
    private final static long ENC_D3 = ENC_MAX * ENC_D2;  // 262144
    private final static long ENC_D4 = ENC_MAX * ENC_D3;  // 16777216
    private final static long ENC_D5 = ENC_MAX * ENC_D4;  // 1073741824
    private final static long ENC_D6 = ENC_MAX * ENC_D5;  // 68719476736

    private final static int ENC_B1 = ENC_BITS;
    private final static int ENC_B2 = ENC_B1 + ENC_BITS;
    private final static int ENC_B3 = ENC_B2 + ENC_BITS;
    private final static int ENC_B4 = ENC_B3 + ENC_BITS;
    private final static int ENC_B5 = ENC_B4 + ENC_BITS;


    // TODO use full available span for ID encoding (base would be 224 instead of 64)
    // TODO reverse order, so most significant parts are at the end (more suitable to backwards search)

    /**
     *
     * @param buf
     * @param offs
     * @param val
     * @return
     */
    public static int idEncode(byte[] buf, int offs, long val) {
        if (val < 0) {
            throw new ZicoException("ID cannot be negative.");
        } if (val < ENC_D1) {
            buf[offs]   = (byte)(val + ENC_OFF);
            return 1;
        }  if (val < ENC_D2) {
            buf[offs]   = (byte)((val >>> ENC_B1) + ENC_OFF);
            buf[offs+1] = (byte)((val & ENC_MASK) + ENC_OFF);
            return 2;
        } else if (val < ENC_D3) {
            buf[offs]   = (byte)((val >>> ENC_B2) + ENC_OFF);
            buf[offs+1] = (byte)(((val >>> ENC_B1) & ENC_MASK) + ENC_OFF);
            buf[offs+2] = (byte)((val & ENC_MASK) + ENC_OFF);
            return 3;
        } else if (val < ENC_D4) {
            buf[offs]   = (byte)((val >>> ENC_B3) + ENC_OFF);
            buf[offs+1] = (byte)(((val >>> ENC_B2) & ENC_MASK) + ENC_OFF);
            buf[offs+2] = (byte)(((val >>> ENC_B1) & ENC_MASK) + ENC_OFF);
            buf[offs+3] = (byte)((val & ENC_MASK) + ENC_OFF);
            return 4;
        } else if (val < ENC_D5) {
            buf[offs] = (byte) ((val >>> ENC_B4) + ENC_OFF);
            buf[offs + 1] = (byte) (((val >>> ENC_B3) & ENC_MASK) + ENC_OFF);
            buf[offs + 2] = (byte) (((val >>> ENC_B2) & ENC_MASK) + ENC_OFF);
            buf[offs + 3] = (byte) (((val >>> ENC_B1) & ENC_MASK) + ENC_OFF);
            buf[offs + 4] = (byte) ((val & ENC_MASK) + ENC_OFF);
            return 5;
        } else if (val < ENC_D6) {
            buf[offs] = (byte) ((val >>> ENC_B5) + ENC_OFF);
            buf[offs + 1] = (byte) (((val >>> ENC_B4) & ENC_MASK) + ENC_OFF);
            buf[offs + 2] = (byte) (((val >>> ENC_B3) & ENC_MASK) + ENC_OFF);
            buf[offs + 3] = (byte) (((val >>> ENC_B2) & ENC_MASK) + ENC_OFF);
            buf[offs + 4] = (byte) (((val >>> ENC_B1) & ENC_MASK) + ENC_OFF);
            buf[offs + 5] = (byte) ((val & ENC_MASK) + ENC_OFF);
            return 6;
        } else {
            throw new ZicoException("ID string too long (possibly malformed).");
        }
    }

    public static String idEncodeStr(long val) {
        byte[] buf = new byte[idLen(val)];
        idEncode(buf, 0, val);
        return new String(buf);
    }


    public static int idLen(long val) {
        if (val < 0) {
            throw new ZicoException("ID cannot be negative.");
        } if (val < ENC_D1) {
            return 1;
        } else if (val < ENC_D2) {
            return 2;
        } else if (val < ENC_D3) {
            return 3;
        } else if (val < ENC_D4) {
            return 4;
        } else if (val < ENC_D5) {
            return 5;
        } else if (val < ENC_D6) {
            return 6;
        } else {
            throw new ZicoException("ID too big.");
        }
    }


    public static long idDecode(byte[] buf) {
        return idDecode(buf, 0, buf.length);
    }


    public static long idDecode(byte[] buf, int offs, int len) {
        switch (len) {
            case 1:
                return (buf[offs] & 0xffL) - ENC_OFF;
            case 2:
                return (((buf[offs] & 0xffL) - ENC_OFF) << ENC_B1)
                    +  (buf[offs+1] & 0xffL) - ENC_OFF;
            case 3:
                return (((buf[offs]  & 0xffL) - ENC_OFF) << ENC_B2)
                    + (((buf[offs+1] & 0xffL) - ENC_OFF) << ENC_B1)
                    +   (buf[offs+2] & 0xffL) - ENC_OFF;
            case 4:
                return (((buf[offs]  & 0xffL) - ENC_OFF) << ENC_B3)
                    + (((buf[offs+1] & 0xffL) - ENC_OFF) << ENC_B2)
                    + (((buf[offs+2] & 0xffL) - ENC_OFF) << ENC_B1)
                    +   (buf[offs+3] & 0xffL) - ENC_OFF;
            case 5:
                return (((buf[offs]  & 0xffL) - ENC_OFF) << ENC_B4)
                    + (((buf[offs+1] & 0xffL) - ENC_OFF) << ENC_B3)
                    + (((buf[offs+2] & 0xffL) - ENC_OFF) << ENC_B2)
                    + (((buf[offs+3] & 0xffL) - ENC_OFF) << ENC_B1)
                    +   (buf[offs+4] & 0xffL) - ENC_OFF;
            case 6:
                return (((buf[offs]  & 0xffL) - ENC_OFF) << ENC_B5)
                    + (((buf[offs+1] & 0xffL) - ENC_OFF) << ENC_B4)
                    + (((buf[offs+2] & 0xffL) - ENC_OFF) << ENC_B3)
                    + (((buf[offs+3] & 0xffL) - ENC_OFF) << ENC_B2)
                    + (((buf[offs+4] & 0xffL) - ENC_OFF) << ENC_B1)
                    +   (buf[offs+5] & 0xffL) - ENC_OFF;
            default:
                throw new ZicoException("Invalid encoding length: " + len);
        }
    }


    public static int termEncode(byte[] obuf, int ooffs, int olen, String text, long id) {
        byte[] ibuf = text.getBytes();
        return termEncode(obuf, ooffs, olen, ibuf, 0, ibuf.length, id);
    }


    public static int termEncode(byte[] obuf, int ooffs, int olen, byte[] ibuf, int ioffs, int ilen, long id) {

        int idl = idLen(id), len = 2 + ilen + idl;
        if (olen - ooffs < len) {
            return -1;
        }

        int pos = ooffs;

        pos += idEncode(obuf, pos, id);
        obuf[pos++] = MARK_ID1;

        System.arraycopy(obuf, pos, ibuf, ioffs, ilen);
        pos += ilen;

        obuf[pos++] = MARK_TXT;

        pos += idEncode(obuf, pos, id);
        obuf[pos] = MARK_ID2;

        return len;
    }

}
