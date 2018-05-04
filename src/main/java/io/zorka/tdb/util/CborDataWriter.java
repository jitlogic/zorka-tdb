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

package io.zorka.tdb.util;

import io.zorka.tdb.ZicoException;
import io.zorka.tdb.ZicoException;

import javax.xml.bind.DatatypeConverter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.zorka.tdb.util.CBOR.*;

/**
 *
 */
public class CborDataWriter {

    protected byte[] buf;
    protected int pos, limit, delta;

    public CborDataWriter(int buflen, int delta) {
        this.buf = new byte[buflen];
        this.pos = 0;
        this.limit = buf.length;
        this.delta = delta;
    }

    public CborDataWriter(byte[] buf) {
        this(buf, 0, buf.length);
    }

    public CborDataWriter(byte[] buf, int offs, int len) {
        this.buf = buf;
        this.pos = offs;
        this.limit = offs + len;
    }


    public void write(int b) {
        if (pos == limit) {
            flush();
        }
        buf[pos++] = (byte)b;
    }


    public void write(byte[] b) {
        write(b, 0, b.length);
    }


    public void write(byte[] b, int offs, int len) {
        while (len > 0) {
            if (pos+len >= limit) {
                flush();
            }
            int l = Math.min(len, limit-pos);
            System.arraycopy(b, offs, buf, pos, l);
            len -= l; pos += l; offs += l;
        }
    }


    public void ensure(int l) {
        while (limit - pos < l) {
            flush();
        }
    }


    public void flush() {
        if (delta > 0) {
            limit += delta;
            this.buf = Arrays.copyOf(this.buf, limit);
        } else {
            throw new ZicoException("Buffer overflow.");
        }
    }


    public void writeUInt(int prefix, int i) {
        ensure(5);
        if (i < 0x18) {
            write(prefix + i);
        } else if (i < 0x100) {
            write(prefix + 0x18);
            write(i & 0xff);
        } else if (i < 0x10000) {
            write(prefix + 0x19);
            write((byte) ((i >> 8) & 0xff));
            write((byte) (i & 0xff));
        } else {
            byte[] b = new byte[5];
            b[0] = (byte) (0x1a + prefix);
            b[1] = (byte) ((i >> 24) & 0xff);
            b[2] = (byte) ((i >> 16) & 0xff);
            b[3] = (byte) ((i >> 8) & 0xff);
            b[4] = (byte) (i & 0xff);
            write(b);
        }
    }


    public void writeULong(int prefix, long l) {
        if (l < Integer.MAX_VALUE) {
            writeUInt(prefix, (int)l);
        } else {
            ensure(9);
            byte[] b = new byte[9];
            b[0] = (byte) (0x1b + prefix);
            b[1] = (byte) ((l >> 56) & 0xff);
            b[2] = (byte) ((l >> 48) & 0xff);
            b[3] = (byte) ((l >> 40) & 0xff);
            b[4] = (byte) ((l >> 32) & 0xff);
            b[5] = (byte) ((l >> 24) & 0xff);
            b[6] = (byte) ((l >> 16) & 0xff);
            b[7] = (byte) ((l >> 8) & 0xff);
            b[8] = (byte)  (l & 0xff);
            write(b);
        }
    }


    public void writeInt(int i) {
        if (i >= 0) {
            writeUInt(0, i);
        } else {
            writeUInt(0x20, Math.abs(i)-1);
        }
    }


    public void writeLong(long l) {
        if (l >= 0) {
            writeULong(0, l);
        } else {
            writeULong(NINT_BASE, Math.abs(l)-1L);
        }
    }


    public void writeBytes(byte[] b) {
        writeUInt(ARR_BASE, b.length);
        write(b);
    }


    public void writeString(String s) {
        byte[] b = s.getBytes();
        writeUInt(STR_BASE, b.length);
        write(b);
    }


    public void writeTag(int tag) {
        writeUInt(TAG_BASE, tag);
    }


    public void writeSimpleToken(int token) {
        writeUInt(CBOR.SIMPLE_BASE, token);
    }


    public void writeObj(Object obj) {
        if (obj == null) {
            write(NULL_CODE);
        } else if (obj.getClass() == Integer.class) {
            writeInt((Integer) obj);
        } else if (obj.getClass() == Long.class) {
            writeLong((Long) obj);
        } else if (obj.getClass() == String.class) {
            writeString((String) obj);
        } else if (obj instanceof CborObject) {
            ((CborObject)obj).write(this);
        } else if (obj instanceof List) {
            List lst = (List)obj;
            writeUInt(ARR_BASE, lst.size());
            for (Object o : lst) {
                writeObj(o);
            }
        } else if (obj instanceof Map) {
            Map<Object,Object> m = (Map<Object,Object>)obj;
            writeUInt(MAP_BASE, m.size());
            for (Map.Entry<Object,Object> e : m.entrySet()) {
                writeObj(e.getKey());
                writeObj(e.getValue());
            }
        }
    }

    public void writeNull() {
        write(NULL_CODE);
    }

    public void writeRawLong(long v, boolean littleEndian) {
        if (littleEndian) {
            write ((int)(v & 0xff));
            write ((int)((v >>> 8) & 0xff));
            write ((int)((v >>> 16) & 0xff));
            write ((int)((v >>> 24) & 0xff));
            write ((int)((v >>> 32) & 0xff));
            write ((int)((v >>> 40) & 0xff));
            write ((int)((v >>> 48) & 0xff));
            write ((int)((v >>> 56) & 0xff));
        } else {
            write ((int)((v >>> 56) & 0xff));
            write ((int)((v >>> 48) & 0xff));
            write ((int)((v >>> 40) & 0xff));
            write ((int)((v >>> 32) & 0xff));
            write ((int)((v >>> 24) & 0xff));
            write ((int)((v >>> 16) & 0xff));
            write ((int)((v >>> 8) & 0xff));
            write ((int)(v & 0xff));
        }
    }


    public void position(int pos) {
        this.pos = pos;
    }

    public int position() {
        return this.pos;
    }

    public void reset() {
        pos = 0;
    }

    public byte[] getBuf() {
        return buf;
    }

    public String toBase64String() {
        if (pos > 0) {
            return DatatypeConverter.printBase64Binary(Arrays.copyOf(this.buf, pos));
        } else {
            return "";
        }
    }
}
