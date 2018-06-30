/*
 * Copyright 2016-2018 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
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

package io.zorka.tdb.store;

import io.zorka.tdb.ZicoException;
import io.zorka.tdb.meta.StructuredTextIndex;
import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.util.CborDataWriter;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Normalizes all strings and translates IDs. Extract
 */
public class TraceDataIndexer implements StatelessDataProcessor, AgentDataProcessor {

    private final static int EXC_DELTA = 128;
    private int[] localExIds = new int[EXC_DELTA], agentExIds = new int[EXC_DELTA];
    private int exIdSize = 0;

    private String traceUUID;
    private int chnum;

    /** This attribute tracks call stack depth.  */
    private int stackDepth;

    private StructuredTextIndex index;
    private StatelessDataProcessor output;
    private CborDataWriter writer;
    private AgentHandler ah;

    private int lastPos = -1, lastMethod = -1;

    private int[] lastIds = new int[64];
    private int lastIdsSize = -1;   // -1 -- collecting last IDs disabled, > 0 -- collecting lastIDs enabled, points to first free slot

    private long lastTstamp = -1;

    private long tstart = Long.MAX_VALUE, tstop = Long.MIN_VALUE;

    private TraceTypeResolver traceTypeResolver;

    // TODO indeksowanie methodId na 'czubku' trace'a

    // TODO aktualizacja duration po uwzględnieniu kolejnych fragmentów trace'a

    private List<ChunkMetadata> mrecs = new ArrayList<>(), mrslt = new ArrayList<>();
    private ChunkMetadata mtop = null;

    private long tstamp;


    public TraceDataIndexer(TraceTypeResolver traceTypeResolver) {
        this.traceTypeResolver = traceTypeResolver;
    }


    public void setup(StructuredTextIndex index, AgentHandler ah, String traceUUID, int chnum,
                      StatelessDataProcessor output, CborDataWriter writer) {
        this.index = index;
        this.output = output;
        this.ah = ah;
        this.traceUUID = traceUUID;
        this.chnum = chnum;
        this.writer = writer;
        this.lastPos = -1;
        mrslt.clear();
        if (mrecs.size() > 0) {
            for (ChunkMetadata md : mrecs) {
                md.clearFlag(ChunkMetadata.TF_INITIAL);
            }
        }
    }


    private int methodRef(int id) {
        return catchId(ah.methodRef(id));
    }

    private int stringRef(int id) {
        return catchId(ah.stringRef(id));
    }

    int exIdRef(int exId) {
        for (int i = 0; i < exIdSize; i++) {
            catchId(localExIds[i]);
            if (exId == agentExIds[i]) {
                return localExIds[i];
            }
        }
        throw new ZicoException("Unknown exception ID (not registered yet ?)");
    }


    private int catchId(int id) {

        // TODO level jeszcze jest niepoprawnie liczony - nie uwzględnia zagnieżdżonych trace'ów

        if (mrecs.size() > 0) {
            for (int i = 0; i < mrecs.size(); i++) {
                mrecs.get(i).catchId(id, stackDepth);
            }
        }

        if (lastIdsSize >= 0) {
            if (lastIdsSize >= lastIds.length) {
                lastIds = Arrays.copyOf(lastIds, lastIds.length * 2);
            }
            lastIds[lastIdsSize++] = id;
        }
        return id;
    }


    /** Flushes IDs collected and disables further collection. */
    private void flushLastIds() {
        if (lastIdsSize > 0 && mtop != null) {
            for (int lid : lastIds) {
                mtop.catchId(lid, stackDepth);
            }
            lastIdsSize = -1;
        }
    }

    public void mpush(ChunkMetadata md) {
        mrecs.add(md);
        mtop = md;
    }

    public void mpop() {
        if (mrecs.size() > 0) {
            ChunkMetadata md = mrecs.get(mrecs.size()-1);
            md.markFlag(ChunkMetadata.TF_FINAL);
            md.setTstop(tstop);
            md.setDuration((md.getTstop()-md.getTstart())/TraceDataFormat.TICKS_IN_SECOND);
            mrslt.add(md);
            mrecs.remove(mrecs.size()-1);
            if (mrecs.size() > 0) {
                mtop = mrecs.get(mrecs.size()-1);
                mtop.addRecs(md.getRecs());
                mtop.addErrors(md.getErrors());
                mtop.addCalls(md.getCalls());
            } else {
                mtop = null;
            }
        }
    }

    @Override
    public void traceStart(int pos) {
        stackDepth++;
        lastPos = writer.position();
        lastIdsSize = 0;  // Reset and enable collecting of lastIds
        if (mtop != null) mtop.addRecs(1);
        output.traceStart(pos);
    }


    @Override
    public void traceEnd() {
        if (mtop != null && mtop.getStackDepth() == stackDepth) {
            mpop();
        }
        stackDepth--;
        lastIdsSize = -1;  // Reset and disable collecting of lastIds
        output.traceEnd();
    }

    private int traceBegin(int tid) {
        int typeId = traceTypeResolver.resolve(index.gets(stringRef(tid)));

        ChunkMetadata md = new ChunkMetadata();
        md.setStackDepth(stackDepth);
        md.setChunkNum(chnum);
        md.setTraceUUID(mtop == null ? traceUUID : UUID.randomUUID().toString());
        md.markFlag(ChunkMetadata.TF_INITIAL);
        md.setStartOffs(lastPos);
        md.setMethodId(lastMethod);
        md.setTstamp(lastTstamp);
        md.setTstart(tstart);

        md.setTypeId(typeId);
        md.addRecs(1);
        mpush(md);

        catchId(lastMethod);
        flushLastIds();

        return mtop.catchTypeId(typeId);
    }

    @Override
    public void traceInfo(int k, long v) {

        switch (k) {
            case TraceDataFormat.TI_TSTAMP:
                if (mtop != null) mtop.catchTstamp(v);
                lastTstamp = v;
                break;
            case TraceDataFormat.TI_DURATION:
                if (mtop != null) mtop.catchDuration(v);
                break;
            case TraceDataFormat.TI_TSTART:
                tstart = v;
                break;
            case TraceDataFormat.TI_TSTOP:
                tstop = v;
                break;
            case TraceDataFormat.TI_TYPE:
                v = traceBegin((int)v);
                break;
            case TraceDataFormat.TI_FLAGS:
                if (0 != (v & TraceDataFormat.TF_ERROR)) {
                    if (mtop != null) mtop.setErrorFlag(true);
                }
                break;
            case TraceDataFormat.TI_METHOD:
                v = methodRef((int)v);
                lastMethod = (int)v;
                break;
            case TraceDataFormat.TI_CALLS:
                if (mtop != null) {
                    mtop.addCalls((int)v);
                }
                break;
        }

        output.traceInfo(k, v); // TODO some values may need to be translated
    }

    public static final AtomicLong EMPTY_ATTR_STRINGS = new AtomicLong();

    private Object translate(Object obj) {
        if (obj instanceof String) {
            String s = (String)obj;
            int id = 0;
            if (s.length() != 0) {
                id = index.add(s);
                catchId(id);
            } else {
                EMPTY_ATTR_STRINGS.incrementAndGet();
            }
            return new ObjectRef(id);
        } else if (obj instanceof ObjectRef) {
            ObjectRef r1 = (ObjectRef) obj, r2 = new ObjectRef(r1.id);
            r2.id = stringRef(r2.id);
            catchId(r2.id); // TODO not sure this is correct - we catch IDs both before and after translation, probably 'before' phase should be discarded
            return r2;
        } else if (obj instanceof Map) {
            Map<Object, Object> m1 = (Map<Object, Object>) obj, m2 = new HashMap<>();
            for (Map.Entry<Object, Object> e : m1.entrySet()) {
                Object k = translate(e.getKey());
                Object v = translate(e.getValue());
                if (k instanceof ObjectRef) {
                    if (v instanceof ObjectRef) {
                        catchId(index.addKRPair(((ObjectRef) k).id, ((ObjectRef) v).id));
                    } else if (v instanceof Integer || v instanceof Long || v instanceof Boolean) {
                        catchId(index.addKVPair(((ObjectRef) k).id, ""+v));
                    }
                }
                m2.put(k, v);
            }
            return m2;
        } else if (obj instanceof List) {
            List<Object> l1 = (List<Object>)obj, l2 = new ArrayList<>(l1.size());
            for (Object o : l1) {
                Object v = translate(o);
                if (v instanceof ObjectRef) {
                    catchId(((ObjectRef)v).id);
                }
                l2.add(v);
            }
            return l2;
        } else {
            return obj;
        }
    }

    @Override
    public void attr(Map<Object, Object> data) {
        Map<Object, Object> d1 = (Map<Object,Object>)translate(data);
        if (mtop != null && mtop.getStackDepth() == stackDepth) {
            mtop.addAttrData(d1);
        }
        output.attr(d1);
    }


    @Override
    public void exceptionRef(int ref) {
        output.exceptionRef(exIdRef(ref));
    }

    public static final AtomicLong EMPTY_EXCEPTION_MSGS = new AtomicLong();

    @Override
    public void exception(ExceptionData ex) {
        int classId = stringRef(ex.classId);
        int msgId = 0;
        if (ex.msg != null) {
            if (ex.msg.length() > 0) {
                msgId = index.add(ex.msg);
            } else {
                EMPTY_EXCEPTION_MSGS.incrementAndGet();
            }
        } else {
            msgId = stringRef(ex.msgId);
        }

        int[] stackIds = new int[ex.stackTrace.size()];

        for (int i = 0; i < stackIds.length; i++) {
            StackData se = ex.stackTrace.get(i);
            stackIds[i] = index.addStackItem(stringRef(se.classId), stringRef(se.methodId), stringRef(se.fileId), se.lineNum);
            catchId(stackIds[i]);
        }

        int stackId = index.addCallStack(stackIds);
        if (stackId > 0) catchId(stackId);

        int causeId = ex.causeId != 0 ? exIdRef(ex.causeId) : 0;
        if (causeId > 0) catchId(causeId);

        int excId = index.addException(classId, msgId, stackId, causeId);
        if (excId > 0) catchId(excId);

        localExIds[exIdSize] = excId;
        agentExIds[exIdSize] = ex.id;

        exIdSize++;

        if (mtop != null) {
            mtop.addErrors(1);
        }

        output.exceptionRef(excId);
    }


    public int getStackDepth() {
        return stackDepth;
    }


    @Override
    public void commit() {
        output.commit();
        index.flush();
    }



    public List<ChunkMetadata> getTraceMetaData() {
        List<ChunkMetadata> rslt = new ArrayList<>();
        rslt.addAll(mrecs);
        rslt.addAll(mrslt);
        return rslt;
    }

    @Override
    public int defStringRef(int remoteId, String s, byte type) {
        return ah.defStringRef(remoteId, s, type);
    }

    @Override
    public int defMethodRef(int remoteId, int classId, int methodId, int signatureId) {
        return ah.defMethodRef(remoteId, classId, methodId, signatureId);
    }

    @Override
    public void defAgentAttr(String agentUUID, String key, String val) {
        ah.defAgentAttr(agentUUID, key, val);
    }

    public long getTstamp() {
        return tstamp;
    }

    public void setTstamp(long tstamp) {
        this.tstamp = tstamp;
    }
}
