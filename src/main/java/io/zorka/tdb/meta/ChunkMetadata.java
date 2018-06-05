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

package io.zorka.tdb.meta;

import java.util.*;

/**
 *
 */
public class ChunkMetadata {

    /** Retrieve also trace attibutes. */
    public static final int SF_ATTRS = 0x01;
    /** Start with oldest traces, not newest (default) */
    public static final int SF_REVERSE = 0x02;
    /** Search only last (active) store. */
    public static final int SF_CURRENT = 0x04;
    /** Error flag  */
    public static final int TF_ERROR = 0x01;
    /** If set, chunk is only part of whole trace. */
    public static final int TF_CHUNKED = 0x02;
    /** Marks first chunk of trace. */
    public static final int TF_INITIAL = 0x04;
    /** Marks final chunk of trace. */
    public static final int TF_FINAL = 0x08;

    private int typeId; // Trace type (eg.
    private int appId;
    private int envId;
    private int tflags;

    private long tstamp;
    private long duration;

    private int chunkNum;

    private long dataOffs;

    private int startOffs;

    private int zeroLevel = 1;

    private int methodId;
    private int descId;

    private int stackDepth;

    private int calls, errors, recs;

    private long tstart, tstop;

    private long uuidLSB, uuidMSB;

    private Set<Integer> fids = null;

    private Set<Integer> tids = null;

    private Map<Object,Object> attrs = null;

    @Override
    public String toString() {
        return "ChunkMetadata(dataOffs=" + dataOffs + ", startOffs=" + startOffs + ", methodId=" + methodId
            + ", tstamp=" + tstamp + ", typeId=" + typeId + ")";
    }

    public ChunkMetadata() { }

    public ChunkMetadata(String traceUUID) {
        setTraceUUID(traceUUID);
    }

    public ChunkMetadata(long uuidLSB, long uuidMSB) {
        this.uuidLSB = uuidLSB;
        this.uuidMSB = uuidMSB;
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

    public int getAppId() {
        return appId;
    }

    public void setAppId(int appId) {
        this.appId = appId;
    }

    public long getTstamp() {
        return tstamp;
    }

    public void setTstamp(long tstamp) {
        this.tstamp = tstamp;
    }

    public String getTraceUUID() {
        UUID uuid = new UUID(uuidMSB, uuidLSB);
        return uuid.toString();
    }

    public void setTraceUUID(String traceUUID) {
        UUID uuid = UUID.fromString(traceUUID);
        uuidLSB = uuid.getLeastSignificantBits();
        uuidMSB = uuid.getMostSignificantBits();
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
        return 0 != (tflags & TF_ERROR);
    }

    public void setErrorFlag(boolean errorFlag) {
        if (errorFlag) {
            tflags |= TF_ERROR;
        } else {
            tflags &= ~TF_ERROR;
        }
    }

    public int getTypeId() {
        return typeId;
    }

    public int getEnvId() {
        return envId;
    }

    public void setEnvId(int envId) {
        this.envId = envId;
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

    public long getUuidLSB() {
        return uuidLSB;
    }

    public void setUuidLSB(long uuidLSB) {
        this.uuidLSB = uuidLSB;
    }

    public long getUuidMSB() {
        return uuidMSB;
    }

    public void setUuidMSB(long uuidMSB) {
        this.uuidMSB = uuidMSB;
    }
}
