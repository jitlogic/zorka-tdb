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

import io.zorka.tdb.ZicoException;

import static io.zorka.tdb.util.BitUtils.UNSAFE;
import static io.zorka.tdb.util.BitUtils.BYTE_ARRAY_OFFS;

import static com.jitlogic.zorka.cbor.CBOR.*;

/**
 *
 */
public class CborBufReader {

    private Object obj = null;

    private long addr, end;
    private long pos;

    // TODO implement safe version of this reader, merge with CborDataReader

    public CborBufReader(byte[] buf) {
        this.obj = buf;
        this.addr = BYTE_ARRAY_OFFS;
        this.end = this.addr + buf.length;
        this.pos = addr;
    }

    public byte[] getRawBytes() {
        if (obj instanceof byte[]) {
            return (byte[])obj;
        } else {
            int len = (int) (end - addr);
            byte[] rslt = new byte[len];
            for (int i = 0; i < len; i++) {
                rslt[i] = UNSAFE.getByte(addr+i);
            }
            return rslt;
        }
    }

    public int position() {
        return (int)(pos - addr);
    }


    public void position(long pos) {
        if (pos < 0 || pos > end-addr) {
            throw new ZicoException("Try ing to set illegal position on direct buf");
        }
        this.pos = addr + pos;
    }


    public int size() { return (int) (end - addr); }


    public byte read() {
        return UNSAFE.getByte(obj, pos++);
    }


    public long readLong() {
        int b = UNSAFE.getByte(obj, pos++) & 0xff, t = b & TYPE_MASK;
        long v = (b & VALU_MASK);

        switch ((int)v) {
            case UINT_CODE1:
                v = UNSAFE.getByte(obj, pos++) & 0xffL;
                break;
            case UINT_CODE2:
                v = ((UNSAFE.getByte(obj, pos++) & 0xffL) << 8) |
                    (UNSAFE.getByte(obj, pos++) & 0xffL);
                break;
            case UINT_CODE4:
                v = ((UNSAFE.getByte(obj, pos++) & 0xffL) << 24) |
                    ((UNSAFE.getByte(obj, pos++) & 0xffL) << 16) |
                    ((UNSAFE.getByte(obj, pos++) & 0xffL) << 8) |
                    ((UNSAFE.getByte(obj, pos++) & 0xffL));
                break;
            case UINT_CODE8:
                v = ((UNSAFE.getByte(obj, pos++) & 0xffL) << 56) |
                    ((UNSAFE.getByte(obj, pos++) & 0xffL) << 48) |
                    ((UNSAFE.getByte(obj, pos++) & 0xffL) << 40) |
                    ((UNSAFE.getByte(obj, pos++) & 0xffL) << 32) |
                    ((UNSAFE.getByte(obj, pos++) & 0xffL) << 24) |
                    ((UNSAFE.getByte(obj, pos++) & 0xffL) << 16) |
                    ((UNSAFE.getByte(obj, pos++) & 0xffL) << 8) |
                    ((UNSAFE.getByte(obj, pos++) & 0xffL));
                break;
            default:
                if (v > UINT_CODE8) {
                    pos--;
                    throw new ZicoException("Invalid prefix code: " + b);
                }
        }

        return t == NINT_BASE ? -v-1 : v;
    }


    public int readInt() {
        int b = UNSAFE.getByte(obj, pos++) & 0xff;
        int v = b & VALU_MASK, t = b & TYPE_MASK;

        switch (v) {
            case UINT_CODE1:
                v = UNSAFE.getByte(obj, pos++) & 0xff;
                break;
            case UINT_CODE2:
                v = ((UNSAFE.getByte(obj, pos++) & 0xff) << 8) |
                    (UNSAFE.getByte(obj, pos++) & 0xff);
                break;
            case UINT_CODE4:
                v = ((UNSAFE.getByte(obj, pos++) & 0xff) << 24) |
                    ((UNSAFE.getByte(obj, pos++) & 0xff) << 16) |
                    ((UNSAFE.getByte(obj, pos++) & 0xff) << 8) |
                    ((UNSAFE.getByte(obj, pos++) & 0xff));
                break;
            case UINT_CODE8:
                pos--;
                throw new ZicoException("Expected int but encountered long at pos " + (pos-addr));
            default:
                if (v > UINT_CODE8) {
                    pos--;
                    throw new ZicoException("Invalid prefix code: " + b);
                }
        }

        return t == NINT_BASE ? -v-1 : v;
    }


    public byte[] readBytes() {
        // TODO handle variable length byte arrays (if needed)
        int type = peekType();
        if (type != BYTES_BASE && type != STR_BASE) {
            throw new ZicoException("Expected byte array but got type=" + type);
        }
        int len = readInt();
        if (len > end - pos) {
            throw new ZicoException("Unexpected end of buffer.");
        }
        byte[] rslt = new byte[len];
        UNSAFE.copyMemory(obj, pos, rslt, BYTE_ARRAY_OFFS, len);
        pos += len;
        return rslt;
    }


    public String readStr() {
        int type = peekType();
        if (type != STR_BASE) {
            throw new ZicoException("Expected string data but got type=" + type);
        }
        byte[] buf = readBytes();
        return new String(buf);
    }

    public int readTag() {
        int type = peekType();
        if (type != TAG_BASE) {
            throw new ZicoException(String.format("Expected tag, got type %02x", type));
        }
        return readInt();
    }

    public int peek() {
        return UNSAFE.getByte(obj, pos) & 0xff;
    }


    public int peekType() {
        return (UNSAFE.getByte(obj, pos) & 0xff) & TYPE_MASK;
    }


    public long readRawLong(boolean littleEndian) {
        if (littleEndian) {
            return (read() & 0xffL)
                | ((read() & 0xffL) << 8)
                | ((read() & 0xffL) << 16)
                | ((read() & 0xffL) << 24)
                | ((read() & 0xffL) << 32)
                | ((read() & 0xffL) << 40)
                | ((read() & 0xffL) << 48)
                | ((read() & 0xffL) << 56);
        } else {
            return ((read() & 0xffL) << 56)
                |  ((read() & 0xffL) << 48)
                |  ((read() & 0xffL) << 40)
                |  ((read() & 0xffL) << 32)
                |  ((read() & 0xffL) << 24)
                |  ((read() & 0xffL) << 16)
                |  ((read() & 0xffL) << 8)
                |   (read() & 0xffL);
        }
    }


}
