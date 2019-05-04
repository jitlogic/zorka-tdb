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

package io.zorka.tdb.text;

import io.zorka.tdb.search.SearchNode;
import io.zorka.tdb.search.ssn.TextNode;
import io.zorka.tdb.search.tsn.KeyValSearchNode;
import io.zorka.tdb.store.ExceptionData;
import io.zorka.tdb.store.StackData;
import io.zorka.tdb.util.BitmapSet;
import io.zorka.tdb.util.ZicoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class StructuredTextIndex extends AbstractTextIndex implements WritableTextIndex {

    private final static Logger log = LoggerFactory.getLogger(WritableTextIndex.class);

    // TODO implement proper escaping here

    public static final byte STRING_TYPE  = 0x00; // Generic string, raw encoding (no prefix);

    public static final byte TYPE_MIN     = 0x04;
    public static final byte KEYWORD_TYPE = 0x04; // LISP keyword:     0x04|keyword_name|0x04
    public static final byte CLASS_TYPE   = 0x05; // Class name        0x05|class_name|0x05
    public static final byte METHOD_TYPE  = 0x06; // Method name       0x06|method_name|0x06
    public static final byte UUID_TYPE    = 0x07; // UUID              0x07|uuid_encoded|0x07
    public static final byte SIGN_TYPE    = 0x08; // Method signature  0x08|method_signature|0x08
    public static final byte TYPE_MAX     = 0x08;

    public static final byte KV_PAIR      = 0x09;
    public static final byte KR_PAIR      = 0x0a;

    public static final byte METHOD_DESC     = 0x0b;  // Method description   0x0b|cid|0x0b|mid|0x0b|sid|0x0b
    public static final byte EXCEPTION_DESC  = 0x0c;
    public static final byte CALL_STACK_DESC = 0x0d;
    public static final byte STACK_ITEM_DESC = 0x0e;
    public static final byte AGENT_ATTR_DESC = 0x0f;


    private WritableTextIndex tidx;

    public StructuredTextIndex(WritableTextIndex tidx) {
        this.tidx = tidx;
    }

    public WritableTextIndex getParentIndex() {
        return tidx;
    }

    public int addStackItem(int classId, int methodId, int fileId, int lineNum) {
        return addTuple4(STACK_ITEM_DESC, classId, methodId, fileId, lineNum);
    }


    public int addCallStack(int...sIds) {
        return addTuple(CALL_STACK_DESC, sIds);
    }

    public int addException(int classId, int msgId, int stackId, int causeId) {
        return addTuple4(EXCEPTION_DESC, classId, msgId, stackId, causeId);
    }


    public int addMethod(int classId, int methodId, int signatureId) {
        return addTuple3(METHOD_DESC, classId, methodId, signatureId);
    }


    public int addKVPair(int keyId, String s) {
        return addKVPair(keyId, s.getBytes());
    }


    public int addKVPair(int keyId, byte[] buf) {
        return addKVPair(keyId, buf, 0, buf.length);
    }


    /**
     * Adds key-value pair to an index.
     * @param keyId key ID (we always normalize keys)
     * @param buf buffer containing value (string or value serialized to string)
     * @param offs offset in buffer where value starts
     * @param len value length (in bytes)
     * @return newly assigned ID
     */
    public int addKVPair(int keyId, byte[] buf, int offs, int len) {
        byte[] pbuf = new byte[12 + len];
        int pos = 0;

        pbuf[pos++] = KV_PAIR;
        pos += RawDictCodec.idEncode(pbuf, pos, keyId);
        pbuf[pos++] = KV_PAIR;
        System.arraycopy(buf, offs, pbuf, pos, len); pos += len;
        pbuf[pos++] = KV_PAIR;

        return add(pbuf, 0, pos);
    }

    public int getKVPair(int keyId, byte[] buf) {
        return getKVPair(keyId, buf, 0, buf.length);
    }

    public int getKVPair(int keyId, byte[] buf, int offs, int len) {
        byte[] pbuf = new byte[12 + len];
        int pos = 0;

        pbuf[pos++] = KV_PAIR;
        pos += RawDictCodec.idEncode(pbuf, pos, keyId);
        pbuf[pos++] = KV_PAIR;
        System.arraycopy(buf, offs, pbuf, pos, len); pos += len;
        pbuf[pos++] = KV_PAIR;

        return get(pbuf, 0, pos);
    }


    public byte[] encKVPair(int keyId, byte[] buf) {
        return encKVPair(keyId, buf, 0, buf.length);
    }

    public byte[] encKVPair(int keyId, byte[] buf, int offs, int len) {
        byte[] pbuf = new byte[12 + len];
        int pos = 0;

        pbuf[pos++] = KV_PAIR;
        pos += RawDictCodec.idEncode(pbuf, pos, keyId);
        pbuf[pos++] = KV_PAIR;
        System.arraycopy(buf, offs, pbuf, pos, len); pos += len;
        pbuf[pos] = KV_PAIR;

        return pbuf;
    }

    /**
     *
     * @param keyRef
     * @param valRef
     * @return
     */
    public int addKRPair(int keyRef, int valRef) {
        return addTuple2(KR_PAIR, keyRef, valRef);
    }


    public int add(byte type, String s) {
        return addTyped(type, s.getBytes());
    }

    public int addTyped(byte type, String s) {
        return addTyped(type, s.getBytes());
    }

    public int addTyped(byte type, byte[] buf) {
        return addTyped(type, buf, 0, buf.length);
    }


    /**
     * Adds typed object
     * @param type object type tag (see *_TYPE constants)
     * @param buf input buffer
     * @param offs input buffer offset
     * @param len data length (must fit inside input buffer)
     * @return
     */
    public int addTyped(byte type, byte[] buf, int offs, int len) {

        if (type == STRING_TYPE) {
            return add(buf, offs, len);
        }

        byte[] tbuf = new byte[len + 2];

        System.arraycopy(buf, offs, tbuf, 1, len);
        tbuf[0] = type;
        tbuf[len+1] = type;

        return add(tbuf, 0, len+2);
    }


    public int getTyped(byte type, String s) {
        return getTyped(type, s.getBytes());
    }


    public int getTyped(byte type, byte[] buf) {
        return getTyped(type, buf, 0, buf.length);
    }


    public int getTyped(byte type, byte[] buf, int offs, int len) {
        byte[] tmp = new byte[len + 2];
        System.arraycopy(buf, offs, tmp, 1, len);
        tmp[0] = type;
        tmp[len+1] = type;
        return get(tmp);
    }

    public int getTuple(byte marker, int...ids) {
        byte[] pbuf = new byte[12 * ids.length + 1];
        int pos = 0;

        pbuf[pos++] = marker;
        for (long sid : ids) {
            pos += RawDictCodec.idEncode(pbuf, pos, sid);
            pbuf[pos++] = marker;
        }

        return get(pbuf, 0, pos);
    }

    private int addTuple(byte marker, int...ids) {
        byte[] pbuf = new byte[12 * ids.length + 1];
        int pos = 0;

        pbuf[pos++] = marker;
        for (long sid : ids) {
            pos += RawDictCodec.idEncode(pbuf, pos, sid);
            pbuf[pos++] = marker;
        }

        return add(pbuf, 0, pos);
    }


    private int addTuple2(byte marker, int id1, int id2) {
        byte[] pbuf = new byte[22];
        int pos = 0;

        pbuf[pos++] = marker;
        pos += RawDictCodec.idEncode(pbuf, pos, id1);
        pbuf[pos++] = marker;
        pos += RawDictCodec.idEncode(pbuf, pos, id2);
        pbuf[pos++] = marker;

        return add(pbuf, 0, pos);
    }


    public byte[] encTuple2(byte marker, int id1, int id2) {
        byte[] pbuf = new byte[22];
        int pos = 0;

        pbuf[pos++] = marker;
        pos += RawDictCodec.idEncode(pbuf, pos, id1);
        pbuf[pos++] = marker;
        pos += RawDictCodec.idEncode(pbuf, pos, id2);
        pbuf[pos++] = marker;

        return Arrays.copyOf(pbuf, pos);
    }


    private int addTuple3(byte marker, int id1, int id2, int id3) {
        byte[] pbuf = new byte[33];
        int pos = 0;

        pbuf[pos++] = marker;
        pos += RawDictCodec.idEncode(pbuf, pos, id1);
        pbuf[pos++] = marker;
        pos += RawDictCodec.idEncode(pbuf, pos, id2);
        pbuf[pos++] = marker;
        pos += RawDictCodec.idEncode(pbuf, pos, id3);
        pbuf[pos++] = marker;

        return add(pbuf, 0, pos);
    }


    private int addTuple4(byte marker, int id1, int id2, int id3, int id4) {
        byte[] pbuf = new byte[44];
        int pos = 0;

        pbuf[pos++] = marker;
        pos += RawDictCodec.idEncode(pbuf, pos, id1);
        pbuf[pos++] = marker;
        pos += RawDictCodec.idEncode(pbuf, pos, id2);
        pbuf[pos++] = marker;
        pos += RawDictCodec.idEncode(pbuf, pos, id3);
        pbuf[pos++] = marker;
        pos += RawDictCodec.idEncode(pbuf, pos, id4);
        pbuf[pos++] = marker;

        return add(pbuf, 0, pos);
    }


    @Override
    public String getPath() {
        return tidx.getPath();
    }

    @Override
    public int getIdBase() {
        return tidx.getIdBase();
    }


    @Override
    public int getNWords() {
        return tidx.getNWords();
    }


    @Override
    public long getDatalen() {
        return tidx.getDatalen();
    }


    @Override
    public byte[] get(int id) {
        return tidx.get(id);
    }


    @Override
    public int get(byte[] buf, int offs, int len, boolean esc) {
        return tidx.get(buf, offs, len, esc);
    }

    @Override
    public long length() {
        return tidx.length();
    }

    @Override
    public int add(byte[] buf, int offs, int len, boolean esc) {
        return tidx.add(buf, offs, len, esc);
    }


    public ExceptionData getExceptionData(int id, boolean getStack) {
        byte [] eib = tidx.get(id);
        ExceptionData rslt = null;
        if (eib != null) {
            int classId = getInt(eib, EXCEPTION_DESC, 0);
            int msgId = getInt(eib, EXCEPTION_DESC, 1);
            int stackId = getInt(eib, EXCEPTION_DESC, 2);
            int causeId = getInt(eib, EXCEPTION_DESC, 3);
            rslt = new ExceptionData(0, classId, msgId, causeId);
            if (getStack) {
                byte[] stb = tidx.get(stackId);
                if (stb != null) {
                    int sz = countMarkers(stb, CALL_STACK_DESC) - 1;
                    for (int i = 0; i < sz; i++) {
                        byte[] sib = tidx.get(getInt(stb, CALL_STACK_DESC, i));
                        if (sib != null) {
                            int cid = getInt(sib, STACK_ITEM_DESC, 0);
                            int mid = getInt(sib, STACK_ITEM_DESC, 1);
                            int fid = getInt(sib, STACK_ITEM_DESC, 2);
                            int lnum = getInt(sib, STACK_ITEM_DESC, 3);
                            rslt.addStackElement(new StackData(cid, mid, fid, lnum));
                        }
                    }
                }
            }
        }
        return rslt;
    }


    /**
     * Resolves and returns encoded string. Automatically
     *
     * @param id
     * @return
     */
    public String resolve(int id) {

        if (id < 0) return null;

        byte[] buf = get(id);

        if (buf == null) return null;

        if (buf.length == 0) return "";

        switch (buf[0]) {
            case KEYWORD_TYPE:
            case CLASS_TYPE:
            case METHOD_TYPE:
            case UUID_TYPE:
            case SIGN_TYPE: {
                byte[] b1 = new byte[buf.length-2];
                System.arraycopy(buf, 1, b1, 0, b1.length);
                return new String(b1);
            }
            case METHOD_DESC: {
                String cname = resolve(getInt(buf, METHOD_DESC, 0));
                String mname = resolve(getInt(buf, METHOD_DESC, 1));
                String msign = resolve(getInt(buf, METHOD_DESC, 2));

                return ZicoUtil.prettyPrint(
                    cname != null ? cname : "UnknownClass",
                    mname != null ? mname : "unknownMethod",
                    msign != null ? msign : "()V");
            }
            case EXCEPTION_DESC:
            case KV_PAIR:
            case KR_PAIR:
            case CALL_STACK_DESC:
            case STACK_ITEM_DESC:
            case AGENT_ATTR_DESC:
                return "<unsupported>";
        }

        return new String(buf);
    }


    @Override
    public void flush() {
        tidx.flush();
    }


    @Override
    public void close() throws IOException {
        tidx.close();
    }


    @Override
    public int search(SearchNode expr, BitmapSet rslt) {

        if (!(expr instanceof KeyValSearchNode)) {
            return tidx.search(expr, rslt);
        }

        int idk = tidx.get(((KeyValSearchNode)expr).getKey());

        if (idk < 0) return 0;

        // Character class search nodes not supported (yet)
        if (!(((KeyValSearchNode)expr).getVal() instanceof TextNode)) return 0;


        TextNode tsn = (TextNode) ((KeyValSearchNode)expr).getVal();

        if (tsn.isMatchStart() && tsn.isMatchEnd()) {
            // Looking for exact match ...
            int idv = tidx.get(tsn.getText());
            if (idv < 0) return 0;

            byte[] buf = encTuple2(KR_PAIR, idk, idv);
            int r = tidx.get(buf);
            if (r >= 0) {
                rslt.set(r);
                return 1;
            } else {
                return 0;
            }
        } else {
            BitmapSet ids = new BitmapSet();
            int sz = tidx.search(tsn, rslt);
            if (sz == 0) return 0;
            int cnt = 0;
            for (int id = ids.first(); id > 0; id = ids.next(id)) {
                byte[] buf = encTuple2(KR_PAIR, idk, id);
                int i = tidx.get(buf);
                if (i > 0 && !rslt.get(i)) {
                    cnt++;
                    rslt.set(i);
                }
            }
            return cnt;
        }
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
