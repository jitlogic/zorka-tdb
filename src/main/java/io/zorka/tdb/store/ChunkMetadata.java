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

package io.zorka.tdb.store;

import com.jitlogic.zorka.cbor.*;
import com.jitlogic.zorka.common.util.ZorkaUtil;
import io.zorka.tdb.ZicoException;
import io.zorka.tdb.util.CborBufReader;

import java.util.*;

import static com.jitlogic.zorka.cbor.TraceDataTags.TAG_CHUNK_METADATA;
import static com.jitlogic.zorka.cbor.TraceRecordFlags.*;

/**
 * Metadata describing given trace chunk. Some attributes are stored in MetadataQuickIndex,
 * other ones are used as interim in trace ingestion process.
 */
public class ChunkMetadata {

    public static final int TYPE_MASK = 0xC0000000;
    public static final int INT_TYPE  = 0x00000000;
    public static final int DBL_TYPE  = 0x40000000;
    public static final int BOOL_TYPE = 0x80000000;
    public static final int NULL_TYPE = 0xC0000000;

    /** Trace ID (high word) */
    private long traceId1;

    /** Trace ID (low word) */
    private long traceId2;

    /** Span ID */
    private long spanId;

    /** Chunk sequential number. */
    private int chunkNum;

    /** Parent ID */
    private long parentId;

    /** Trace flags. See com.jitlogic.zorka.cbor.TraceRecordFlags for reference. */
    private int tflags;

    /** Trace timestamp (time when trace started in milliseconds since Epoch). */
    private long tstamp;

    /** Trace duration (in seconds). */ // TODO milliseconds/microseconds/ticks
    private long duration;

    /** Position of saved chunk inside trace data file. */
    private long dataOffs;

    /** Start offset of trace inside data chunk. For top level traces it should always be 0. */
    private int startOffs;

    private int zeroLevel = 1;

    /** Initial stack depth (should be 0 for top level traces, more than 0 for all embedded traces). */
    private int stackDepth;

    /** Number of method calls processed (but not always recorded) by tracer. */
    private int calls;

    /** Number of errors registered by tracer. */
    private int errors;

    /** Number of method calls recorded by tracer. */
    private int recs;

    /** Chunk start timestamp (ticks since trace start). */
    private long tstart;

    /** Chunk end timestamp (ticks since trace start). */
    private long tstop;

    private boolean hasChildren;

    /** String attributes */
    private Map<Integer,Integer> sattrs = new TreeMap<>();

    /** Primitive Attributes (numeric, booleans) */
    private Map<Integer,Long> nattrs = new TreeMap<>();

    /** Store owning this chunk. */
    private transient SimpleTraceStore store;

    /** Plain text (resolved) trace attributes map. */
    private transient Map<String,Object> attributes;

    /** If there are more chunks in this span, this list contains them */
    private transient List<ChunkMetadata> chunks = null;

    /** Children spans. */
    private transient List<ChunkMetadata> children = null;

    @Override
    public String toString() {
        return "ChunkMetadata(tid=" + getTraceIdHex() + ",sid=" + getSpanIdHex() + ",chn=" + getChunkNum()
            + ",pid=" + getParentIdHex() + ")";
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

    public SimpleTraceStore getStore() {
        return store;
    }

    public void setStore(SimpleTraceStore store) {
        this.store = store;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
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

    public boolean hasError() {
        return 0 != (tflags & TF_ERROR_MARK);
    }

    public void setError(boolean errorFlag) {
        if (errorFlag) {
            tflags |= TF_ERROR_MARK;
        } else {
            tflags &= ~TF_ERROR_MARK;
        }
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

    public Map<Integer, Integer> getSattrs() {
        return sattrs;
    }

    public Map<Integer, Long> getNattrs() {
        return nattrs;
    }

    public List<ChunkMetadata> getChunks() {
        return chunks;
    }

    public void setChunks(List<ChunkMetadata> chunks) {
        this.chunks = chunks;
    }

    public List<ChunkMetadata> getChildren() {
        return children;
    }

    public void setChildren(List<ChunkMetadata> children) {
        this.children = children;
    }

    public void setHasChildren(boolean hasChildren) {
        this.hasChildren = hasChildren;
    }

    public boolean isHasChildren() {
        return hasChildren || (children != null && !children.isEmpty());
    }

    public static byte[] serialize(ChunkMetadata cm) {
        CborDataWriter w = new CborDataWriter(192,128);
        w.writeTag(TAG_CHUNK_METADATA);
        w.writeLong(cm.traceId1);
        w.writeLong(cm.traceId2);
        w.writeLong(cm.parentId);
        w.writeLong(cm.spanId);
        w.writeInt(cm.chunkNum);

        w.writeInt(cm.tflags);
        w.writeLong(cm.tstamp);
        w.writeLong(cm.duration);
        w.writeLong(cm.dataOffs);
        w.writeInt(cm.startOffs);

        w.writeInt(cm.zeroLevel);
        w.writeInt(cm.stackDepth);

        w.writeInt(cm.calls);
        w.writeInt(cm.errors);
        w.writeInt(cm.recs);
        w.writeLong(cm.tstart);
        w.writeLong(cm.tstop);

        w.writeObj(cm.getSattrs());
        w.writeObj(cm.getNattrs());

        return w.toByteArray();
    }

    public static ChunkMetadata deserialize(byte[] buf) {

        if (buf == null || buf.length == 0) {
            throw new ZicoException("Tried to deserialize from null or empty buffer");
        }

        CborBufReader r = new CborBufReader(buf);
        int tag = r.readTag();
        if (tag != TAG_CHUNK_METADATA) {
            throw new ZicoException(String.format("Expected TAG_CHUNK_METADATA, got %02x", tag));
        }

        ChunkMetadata cm = new ChunkMetadata(r.readLong(), r.readLong(), r.readLong(), r.readLong(), r.readInt());

        cm.tflags = r.readInt();
        cm.tstamp = r.readLong();
        cm.duration = r.readLong();
        cm.dataOffs = r.readLong();
        cm.startOffs = r.readInt();

        cm.zeroLevel = r.readInt();
        cm.stackDepth = r.readInt();

        cm.calls = r.readInt();
        cm.errors = r.readInt();
        cm.recs = r.readInt();
        cm.tstart = r.readLong();
        cm.tstop = r.readLong();

        int t = r.peekType();
        if (t == CBOR.MAP_BASE) {
            t = r.readInt();
            for (int i = 0; i < t; i++) {
                cm.getSattrs().put(r.readInt(),r.readInt());
            }

        } else {
            throw new ZicoException(String.format("Expected map or NULL, got %02x", t));
        }

        t = r.peekType();
        if (t == CBOR.MAP_BASE) {
            t = r.readInt();
            for (int i = 0; i < t; i++) {
                cm.getNattrs().put(r.readInt(),r.readLong());
            }
        } else {
            throw new ZicoException(String.format("Expected map or NULL, got %02x", t));
        }

        return cm;
    }
}
