package io.zorka.tdb.store;

import com.jitlogic.zorka.common.util.ZorkaUtil;
import io.zorka.tdb.meta.ChunkMetadata;

public class TraceSearchResultItem {

    private long chunkId;

    private long tstamp;

    private long duration;

    private int typeId;

    private int appId;

    private int envId;

    private int hostId;

    private int tflags;

    private int recs;

    private int calls;

    private int errors;

    private long dataOffs;

    private long startOffs;

    private String description;

    private long traceId1;

    private long traceId2;

    private long spanId;

    private long parentId;

    private boolean dtraceOut;

    public TraceSearchResultItem(long chunkId, ChunkMetadata cm) {
        this.tstamp = cm.getTstamp();
        this.duration = cm.getDuration();
        this.typeId = cm.getTypeId();
        this.appId = cm.getAppId();
        this.envId = cm.getEnvId();
        this.hostId = cm.getHostId();

        // TODO tflags

        this.recs = cm.getRecs();
        this.calls = cm.getCalls();
        this.errors = cm.getErrors();
        this.dataOffs = cm.getDataOffs();
        this.startOffs = cm.getStartOffs();

    }

    @Override
    public String toString() {
        return "T[" + description + "]";
    }

    public long getChunkId() {
        return chunkId;
    }

    public void setChunkId(long chunkId) {
        this.chunkId = chunkId;
    }

    public long getTstamp() {
        return tstamp;
    }

    public void setTstamp(long tstamp) {
        this.tstamp = tstamp;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public int getTypeId() {
        return typeId;
    }

    public void setTypeId(int typeId) {
        this.typeId = typeId;
    }

    public int getAppId() {
        return appId;
    }

    public void setAppId(int appId) {
        this.appId = appId;
    }

    public int getEnvId() {
        return envId;
    }

    public void setEnvId(int envId) {
        this.envId = envId;
    }

    public int getHostId() {
        return hostId;
    }

    public void setHostId(int hostId) {
        this.hostId = hostId;
    }

    public int getTflags() {
        return tflags;
    }

    public void setTflags(int tflags) {
        this.tflags = tflags;
    }

    public int getRecs() {
        return recs;
    }

    public void setRecs(int recs) {
        this.recs = recs;
    }

    public int getCalls() {
        return calls;
    }

    public void setCalls(int calls) {
        this.calls = calls;
    }

    public int getErrors() {
        return errors;
    }

    public void setErrors(int errors) {
        this.errors = errors;
    }

    public long getDataOffs() {
        return dataOffs;
    }

    public void setDataOffs(long dataOffs) {
        this.dataOffs = dataOffs;
    }

    public long getStartOffs() {
        return startOffs;
    }

    public void setStartOffs(long startOffs) {
        this.startOffs = startOffs;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean isDtraceOut() {
        return dtraceOut;
    }

    public void setDtraceOut(boolean dtraceOut) {
        this.dtraceOut = dtraceOut;
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

    public long getSpanId() {
        return spanId;
    }

    public void setSpanId(long spanId) {
        this.spanId = spanId;
    }

    public long getParentId() {
        return parentId;
    }

    public void setParentId(long parentId) {
        this.parentId = parentId;
    }

    public String getTraceIdHex() {
        return traceId2 != 0 ? ZorkaUtil.hex(traceId1, traceId2) : ZorkaUtil.hex(traceId1);
    }

    public String getSpanIdHex() {
        return spanId != 0 ? ZorkaUtil.hex(spanId) : null;
    }

    public String getParentIdHex() {
        return parentId != 0 ? ZorkaUtil.hex(parentId) : null;
    }

}
