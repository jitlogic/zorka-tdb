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

package io.zorka.tdb.test.support;

import com.jitlogic.zorka.cbor.CborDataWriter;
import io.zorka.tdb.meta.StructuredTextIndex;
import io.zorka.tdb.store.ExceptionData;
import io.zorka.tdb.store.StackData;

import javax.xml.bind.DatatypeConverter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.jitlogic.zorka.cbor.CBOR.*;
import static com.jitlogic.zorka.cbor.TraceDataTags.*;


/**
 * Trace building utilities used by
 */
public class TraceTestDataBuilder {

    public static class CborTestWriter extends CborDataWriter {

        private List<byte[]> results = new ArrayList<>();

        public CborTestWriter() {
            super(512, 512);
        }

        public void breakAndReset() {
            if (pos > 0) {
                results.add(Arrays.copyOf(buf, pos));
                pos = 0;
            }
        }

        public List<byte[]> getResults() {
            return results;
        }
    } // CborTestWriter


    public interface WireObj {
        void serialize(CborTestWriter w);
    }


    public static class TaggedObjs implements WireObj {

        private int type = ARR_BASE;
        private int tag;
        private Object[] objs;

        public TaggedObjs(int type, int tag, Object...objs) {
            this.tag = tag;
            this.type = type;
            this.objs = objs;
        }

        @Override
        public void serialize(CborTestWriter w) {
                w.writeTag(tag);
                w.writeUInt(type, type == ARR_BASE ? objs.length : objs.length / 2);
                for (Object o : objs) {
                    if (o instanceof WireObj) {
                        ((WireObj)o).serialize(w);
                    } else {
                        w.writeObj(o);
                    }
                }
            }
    }


    public static class TaggedInt implements WireObj {

        private int tag;
        private int val;

        public TaggedInt(int tag, int val) {
            this.tag = tag;
            this.val = val;
        }

        @Override
        public void serialize(CborTestWriter w) {
            w.writeTag(tag);
            w.writeInt(val);
        }
    }


    public final static byte TC = StructuredTextIndex.CLASS_TYPE; // Class name
    public final static byte TM = StructuredTextIndex.METHOD_TYPE; // Method name
    public final static byte TS = StructuredTextIndex.SIGN_TYPE; // Method signature


    public static class TraceObj implements WireObj {

        private boolean le;
        private long mid, calls;
        private long tstart, tstop;
        private WireObj[] objs;

        public TraceObj(boolean le, int mid, long tstart, long tstop, long calls, WireObj...objs) {
            this.le = le;
            this.mid = mid;
            this.tstart = tstart;
            this.tstop = tstop;
            this.calls = calls;
            this.objs = objs;
        }

        @Override
        public void serialize(CborTestWriter w) {
            w.writeTag(TAG_TRACE_START);
            w.write(ARR_VCODE);
            w.writeTag(le ? TAG_PROLOG_LE : TAG_PROLOG_BE);
            w.writeUInt(BYTES_BASE, 8);
            w.writeRawLong((tstart & 0x000000ffffffffffL) | (mid << 40), le);

            for (WireObj obj : objs) {
                obj.serialize(w);
            }

            w.writeTag(le ? TAG_EPILOG_LE : TAG_EPILOG_BE);

            if (calls <= 0x00ffffff) {
                w.writeUInt(BYTES_BASE, 8);
                w.writeRawLong((tstop & 0x000000ffffffffffL) | (calls << 40), le);
            } else {
                w.writeUInt(BYTES_BASE, 16);
                w.writeRawLong(tstop & 0x000000ffffffffffL, le);
                w.writeRawLong(calls, le);
            }

            w.write(BREAK_CODE);

        }
    }

    public static class ExceptionObj implements WireObj {

        private ExceptionData e;

        public ExceptionObj(ExceptionData e) {
            this.e = e;
        }

        @Override
        public void serialize(CborTestWriter w) {
            w.writeTag(TAG_EXCEPTION);
            w.writeUInt(ARR_BASE, 5);
            w.writeInt(e.getId());
            w.writeTag(TAG_STRING_REF);
            w.writeInt(e.getClassId());
            w.writeString(e.getMsg());
            w.writeInt(0); // Cause is empty (?)
            w.writeUInt(ARR_BASE, e.getStackTrace().size());
            for (StackData sd : e.getStackTrace()) {
                w.writeUInt(ARR_BASE, 4);
                w.writeInt(sd.getClassId());
                w.writeInt(sd.getMethodId());
                w.writeInt(sd.getFileId());
                w.writeInt(sd.getLineNum());
            }
        }
    }


    public static class BreakObj implements WireObj {
        @Override
        public void serialize(CborTestWriter w) {
            w.breakAndReset();
        }
    }


    /** Generates agent attributes. */
    public static WireObj aa(Object k, Object v) {
        return new TaggedObjs(ARR_BASE, TAG_AGENT_ATTR, k, v);
    }


    public static WireObj sr(int id, String s, int type) {
        return new TaggedObjs(ARR_BASE, TAG_STRING_DEF, id, s, type);
    }


    public static WireObj mr(int id, int cid, int mid, int sid) {
        return new TaggedObjs(ARR_BASE, TAG_METHOD_DEF, id, cid, mid, sid);
    }


    /** Generates trace begin marker. */
    public static WireObj tb(long clock, int tid) {
        return new TaggedObjs(ARR_BASE, TAG_TRACE_BEGIN, clock, tid);
    }


    /** Generates trace record attributes. */
    public static WireObj ta(Object...attrs) {
        return new TaggedObjs(MAP_BASE, TAG_TRACE_ATTR, attrs);
    }

    public static WireObj ti(int tag, int val) {
        return new TaggedInt(tag, val);
    }

    /** Generates trace record. */
    public static WireObj tr(boolean le, int mid, long tstart, long tstop, int calls, WireObj...objs) {
        return new TraceObj(le, mid, tstart, tstop, calls, objs);
    }

    public static WireObj tf(int flag) {
        return new TaggedInt(TAG_TRACE_FLAGS, flag);
    }

    public static WireObj brk() {
        return new BreakObj();
    }

    public static WireObj ex(int id, int classId, String msg, StackData...sds) {
        ExceptionData e = new ExceptionData(0, classId, msg, 0);

        for (StackData sd : sds) {
            e.addStackElement(sd);
        }

        return new ExceptionObj(e);
    }

    public static StackData sd(int classId, int methodId, int fileId, int lineNum) {
        return new StackData(classId, methodId, fileId, lineNum);
    }

    public static List<byte[]> str(List<WireObj> objs) {
        CborTestWriter w = new CborTestWriter();
        for (WireObj to : objs) {
            to.serialize(w);
        }
        w.breakAndReset();
        return w.getResults();
    }


    public static List<byte[]> str(WireObj...objs) {
        CborTestWriter w = new CborTestWriter();
        for (WireObj to : objs) {
            to.serialize(w);
        }
        w.breakAndReset();
        return w.getResults();
    }


    private static final int TCO = 100;
    private static final String[] TCV = {
        "com.myapp.MyClass",
        "org.catalina.request.Request",
        "org.catalina.Server" };

    private static final int TMO = 200;
    private static final String[] TMV = {
        "myMethod", "invoke", "getStatus", "process", ".init"
    };

    private static final int TSO = 300;
    private static final String[] TSV = {
        "()V", "(II)I", "(I)V",
    };

    private static final int TTO = 0;
    private static final String[] TTV = {
      "HTTP", "SQL", "LDAP", "SOAP", "URI", "STATUS", "MyClass.java", "Request.java", "Server.java"
    };

    public static byte[] agentData() {
        List<WireObj> syms = new ArrayList<>();
        for (int i = 0; i < TCV.length; i++) syms.add(sr(TCO+i, TCV[i], TC));
        for (int i = 0; i < TMV.length; i++) syms.add(sr(TMO+i, TMV[i], TM));
        for (int i = 0; i < TSV.length; i++) syms.add(sr(TSO+i, TSV[i], TS));
        for (int i = 0; i < TTV.length; i++) syms.add(sr(TTO+i, TTV[i], 0));
        for (int ic = 0; ic < TCV.length; ic++)
            for (int im = 0; im < TMV.length; im++)
                for (int is = 0; is < TSV.length; is++)
                    syms.add(mr(mid(ic,im,is), TCO+ic, TMO+im, TSO+is));
        return str(syms).get(0);
    }

    public static int sid(String s) {
        for (int i = 0; i < TTV.length; i++) {
            if (TTV[i].equals(s)) return i;
        }
        return -1;
    }

    public static int mid(int ic, int im, int is) {
        return 1000 + (ic * TCV.length + im) * TMV.length + is;
    }

    public static byte[] trc(int tst, int dur) {
        return str(
            tr(true, mid(0,0,0), tst, tst + dur, 1,
                ta("XXX", "YYY"),
                tb(1500, 0)
            )).get(0);
    }


    public static byte[] trc(int tst, int dur, String k, String v) {
        return str(
            tr(true, mid(0,0,0), tst, tst + dur, 1,
                ta(k, v),
                tb(1500, 0)
            )).get(0);
    }


    public static byte[] trc(int tst, int dur, String k1, String v1, String k2, String v2) {
        return str(
            tr(true, mid(0,0,0), tst, tst + dur, 2,
                tb(1500, 0),
                ta(k1, v1),
                tr(true, mid(1, 1, 1), tst, tst+dur/2, 1,
                    ta(k2, v2))
            )).get(0);
    }


    public static byte[] trc2(int tst, int dur) {
        return str(
            tr(true, mid(0,0,0), tst, tst + dur, 1,
                ta("XXX", "YYY"),
                tb(1500, 0),
                tr(true, mid(0, 0, 0), 100, 120, 1),
                tr(true, mid(0, 0, 0), 120, 140, 1)
            )).get(0);
    }

}
