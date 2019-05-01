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

package io.zorka.tdb.meta;

import com.jitlogic.zorka.common.util.ZorkaUtil;

import java.util.*;

import static com.jitlogic.zorka.cbor.TraceRecordFlags.*;

/**
 * Metadata describing given trace chunk. Some attributes are stored in MetadataQuickIndex,
 * other ones are used as interim in trace ingestion process.
 */
public class ChunkMetadata {

    /** Trace type ID. This is resolved externally and is consistent across all stores. */
    private int typeId;

    /** Trace flags. See com.jitlogic.zorka.cbor.TraceRecordFlags for reference. */
    private int tflags;

    /** Trace timestamp (time when trace started in milliseconds since Epoch). */
    private long tstamp;

    /** Trace duration (in seconds). */
    private long duration;

    /** Chunk sequential number. */
    private int chunkNum;

    /** Position of saved chunk inside trace data file. */
    private long dataOffs;

    /** Start offset of trace inside data chunk. For top level traces it should always be 0. */
    private int startOffs;

    private int zeroLevel = 1;

    /** Top level method (resolved via text index). */
    private int methodId;

    /** Trace description (resolved via text index). */
    private int descId;

    /** Initial stack depth (should be 0 for top level traces, more than 0 for all embedded traces). */
    private int stackDepth;

    /** Number of method calls processed (but not always recorded) by tracer. */
    private int calls;

    /** Number of errors registered by tracer. */
    private int errors;

    /** Number of method calls recorded by tracer. */
    private int recs;

    /** Start timestamp (ticks since trace start). */
    private long tstart;

    /** End timestamp (ticks since trace stop). */
    private long tstop;

    /** Trace ID (high word) */
    private long traceId1;

    /** Trace ID (low word) */
    private long traceId2;

    /** Span ID */
    private long spanId;

    /** Parent ID */
    private long parentId;

    /** Full search IDs */
    private Set<Integer> fids = null;

    /** Top level IDs */
    private Set<Integer> tids = null;

    /** Trace attributes (if resolved) */
    private Map<Object,Object> attrs = null;

    @Override
    public String toString() {
        return "ChunkMetadata(dataOffs=" + dataOffs + ", startOffs=" + startOffs + ", methodId=" + methodId
            + ", tstamp=" + tstamp + ", typeId=" + typeId + ")";
    }

    public ChunkMetadata(long traceId1, long traceId2, long parentId, long spanId, int chunkNum) {
        this.traceId1 = traceId1;
        this.traceId2 = traceId2;
        this.parentId = parentId;
        this.chunkNum = chunkNum;
        this.spanId = spanId;
    }

    public ChunkMetadata(int zeroLevel) {
        this.zeroLevel = zeroLevel;
    }

    public void addAttrData(Map<Object,Object> data) {
        if (attrs == null) attrs = new HashMap<>();
        attrs.putAll(data);
    }

    public Map<Object,Object> getAttrs() {
        return attrs;
    }

    public int getFlags() {
        return tflags;
    }

    public void setFlags(int tflags) {
        this.tflags = tflags;
    }

    public long getTstamp() {
        return tstamp;
    }

    public void setTstamp(long tstamp) {
        this.tstamp = tstamp;
    }



    public long catchTstamp(long tstamp) {
        this.tstamp = tstamp;
        return tstamp;
    }


    public void setDuration(long duration) {
        this.duration = duration;
    }

    public long getDuration() {
        return duration;
    }

    public long catchDuration(long duration) {
        this.duration = duration;
        return duration;
    }

    public boolean hasFlag(int flag) {
        return flag == (tflags & flag);
    }

    public void markFlag(int flag) {
        tflags |= flag;
    }

    public void clearFlag(int flag) {
        tflags &= ~flag;
    }

    public boolean isErrorFlag() {
        return 0 != (tflags & TF_ERROR_MARK);
    }

    public void setErrorFlag(boolean errorFlag) {
        if (errorFlag) {
            tflags |= TF_ERROR_MARK;
        } else {
            tflags &= ~TF_ERROR_MARK;
        }
    }

    public int getTypeId() {
        return typeId;
    }

    public void setTypeId(int typeId) {
        this.typeId = typeId;
    }

    public int getChunkNum() {
        return chunkNum;
    }

    public void setChunkNum(int chunkNum) {
        this.chunkNum = chunkNum;
    }

    public long getDataOffs() {
        return dataOffs;
    }

    public void setDataOffs(long dataOffs) {
        this.dataOffs = dataOffs;
    }

    public int getStartOffs() {
        return startOffs;
    }

    public void setStartOffs(int startOffs) {
        this.startOffs = startOffs;
    }

    public int getMethodId() {
        return methodId;
    }

    public void setMethodId(int methodId) {
        this.methodId = methodId;
    }

    public int getDescId() {
        return descId;
    }

    public void setDescId(int descId) {
        this.descId = descId;
    }

    public int catchTypeId(int typeId) {
        this.typeId = typeId;
        return typeId;
    }

    public int getStackDepth() {
        return stackDepth;
    }

    public void setStackDepth(int stackDepth) {
        this.stackDepth = stackDepth;
    }

    public int getCalls() {
        return calls;
    }

    public void addCalls(int calls) {
        this.calls = Math.max(this.calls, calls);
    }

    public void setCalls(int calls) {
        this.calls = calls;
    }

    public int getErrors() {
        return errors;
    }

    public void addErrors(int errors) {
        this.errors += errors;
    }

    public void setErrors(int errors) {
        this.errors = errors;
    }

    public int getRecs() {
        return recs;
    }

    public void addRecs(int recs) {
        this.recs += recs;
    }

    public void setRecs(int recs) {
        this.recs = recs;
    }

    public long getTstart() {
        return tstart;
    }

    public void setTstart(long tstart) {
        this.tstart = tstart;
    }

    public long getTstop() {
        return tstop;
    }

    public void setTstop(long tstop) {
        this.tstop = tstop;
    }

    public List<Integer> getFids() {
        List<Integer> lst = new ArrayList<>(fids != null ? fids.size() : 1);
        if (fids != null) lst.addAll(fids);
        Collections.sort(lst);
        return lst;
    }

    public List<Integer> getTids() {
        List<Integer> lst = new ArrayList<>(tids != null ? tids.size() : 1);
        if (tids != null) lst.addAll(tids);
        Collections.sort(lst);
        return lst;
    }

    public void catchId(int id, int level) {
        if (tids == null) tids = new HashSet<>();
        if (fids == null) fids = new HashSet<>();
        if (level == zeroLevel) {
            this.tids.add(id);
        }
        this.fids.add(id);
    }

    public long getTraceId1() {
        return traceId1;
    }

    public void setTraceId1(long traceId1) {
        this.traceId1 = traceId1;
    }

    public long getTraceId2() {
        return traceId2;
    }

    public void setTraceId2(long traceId2) {
        this.traceId2 = traceId2;
    }

    public String getTraceIdHex() {
        return traceId2 != 0 ? ZorkaUtil.hex(traceId1, traceId2) : ZorkaUtil.hex(traceId1);
    }

    public long getSpanId() {
        return spanId;
    }

    public String getSpanIdHex() {
        return spanId != 0 ? ZorkaUtil.hex(spanId) : null;
    }

    public void setSpanId(long spanId) {
        this.spanId = spanId;
    }

    public long getParentId() {
        return parentId;
    }

    public String getParentIdHex() {
        return parentId != 0 ? ZorkaUtil.hex(parentId) : null;
    }

    public void setParentId(long parentId) {
        this.parentId = parentId;
    }
}
