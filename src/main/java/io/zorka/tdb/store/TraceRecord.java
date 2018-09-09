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

import io.zorka.tdb.meta.ChunkMetadata;

import java.util.List;
import java.util.Map;

import static com.jitlogic.zorka.cbor.TraceRecordFlags.TF_ERROR_MARK;

/**
 * Intermediate representation of retrieved trace record.
 */
public class TraceRecord {

    /** Start timestamp (ticks since VM start) */
    private long tstart;

    /** End timestamp (ticks since VM start) */
    private long tstop;

    /** Timestamp (wall clock time) */
    private long tstamp;

    /** Duration (ticks) */
    private long duration;

    /** Method descriptor ID (to be retrieved from  */
    private int mid;

    /** Logical position (across multiple chunks) */
    private int pos;

    /** Method descriptor (as string) */
    private String method;

    private int tid;
    private int eid;

    private int flags;

    /** Trace type ID */
    private int type;

    private int level;

    private long recs, errors;

    private long tpos, toffs;

    private ExceptionData exceptionData;

    public TraceRecord(int pos) {
        this.pos = pos;
    }

    public long getNcalls() {
        return ncalls;
    }

    public void setNcalls(long ncalls) {
        this.ncalls = ncalls;
    }

    private long ncalls;
    private Map<Object,Object> attrs;
    private List<Object> children;

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

    public int getMid() {
        return mid;
    }

    public void setMid(int mid) {
        this.mid = mid;
    }

    public Map<Object, Object> getAttrs() {
        return attrs;
    }

    public void setAttrs(Map<Object, Object> attrs) {
        this.attrs = attrs;
    }

    public List<Object> getChildren() {
        return children;
    }

    public void setChildren(List<Object> children) {
        this.children = children;
    }

    public long getTstamp() {
        return tstamp;
    }

    public void setTstamp(long tstamp) {
        this.tstamp = tstamp;
    }

    public int getTid() {
        return tid;
    }

    public void setTid(int tid) {
        this.tid = tid;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public long getRecs() {
        return recs;
    }

    public void setRecs(long recs) {
        this.recs = recs;
    }

    public long getErrors() {
        return errors;
    }

    public void setErrors(long errors) {
        this.errors = errors;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getEid() {
        return eid;
    }

    public void setEid(int eid) {
        this.eid = eid;
    }

    public ExceptionData getExceptionData() {
        return exceptionData;
    }

    public void setExceptionData(ExceptionData exceptionData) {
        this.exceptionData = exceptionData;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public long getTpos() {
        return tpos;
    }

    public void setTpos(long tpos) {
        this.tpos = tpos;
    }

    public long getToffs() {
        return toffs;
    }

    public void setToffs(long toffs) {
        this.toffs = toffs;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public boolean hasError() {
        return 0 != (flags & TF_ERROR_MARK);
    }
}
