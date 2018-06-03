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

package io.zorka.tdb.search;

import io.zorka.tdb.search.ssn.StringSearchNode;

import java.util.Objects;

/**
 * Contains arguments for quick index search. This is general purpose variant,
 * some specific implementations can have subset of below attributes
 */
public class QmiNode implements SearchNode {

    /** Application ID (0 to skip search by appId) */
    private int appId;

    /** Environment ID (0 to skip search by envId) */
    private int envId;

    /** Trace type */
    private int typeId;

    /** Trace duration (milliseconds) */
    private long minDuration = 0, maxDuration = Long.MAX_VALUE;

    /** Start and stop timestamps */
    private long tstart = 0, tstop = Long.MAX_VALUE;

    /** All traces or only errors */
    private boolean errorFlag = false;

    /** Looking for records with specific description ID (useful for some forms of grouping). */
    private StringSearchNode desc;

    /** Minimum and maximum number of calls registered by tracer. */
    private long minCalls = 0, maxCalls = Long.MAX_VALUE;

    /** Minimum and maximum number of records */
    private long minRecs = 0, maxRecs = Long.MAX_VALUE;

    /** Minimum and maximum number of errors. */
    private long minErrs = 0, maxErrs = Long.MAX_VALUE;

    /** Trace UUID */
    private String uuid;

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

    public int getTypeId() {
        return typeId;
    }

    public void setTypeId(int typeId) {
        this.typeId = typeId;
    }

    public long getMinDuration() {
        return minDuration;
    }

    public void setMinDuration(long minDuration) {
        this.minDuration = minDuration;
    }

    public long getMaxDuration() {
        return maxDuration;
    }

    public void setMaxDuration(long maxDuration) {
        this.maxDuration = maxDuration;
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

    public boolean isErrorFlag() {
        return errorFlag;
    }

    public void setErrorFlag(boolean errorFlag) {
        this.errorFlag = errorFlag;
    }

    public long getMinCalls() {
        return minCalls;
    }

    public void setMinCalls(long minCalls) {
        this.minCalls = minCalls;
    }

    public long getMaxCalls() {
        return maxCalls;
    }

    public void setMaxCalls(long maxCalls) {
        this.maxCalls = maxCalls;
    }

    public long getMinRecs() {
        return minRecs;
    }

    public void setMinRecs(long minRecs) {
        this.minRecs = minRecs;
    }

    public long getMaxRecs() {
        return maxRecs;
    }

    public void setMaxRecs(long maxRecs) {
        this.maxRecs = maxRecs;
    }

    public long getMinErrs() {
        return minErrs;
    }

    public void setMinErrs(long minErrs) {
        this.minErrs = minErrs;
    }

    public long getMaxErrs() {
        return maxErrs;
    }

    public void setMaxErrs(long maxErrs) {
        this.maxErrs = maxErrs;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public StringSearchNode getDesc() {
        return desc;
    }

    public void setDesc(StringSearchNode desc) {
        this.desc = desc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QmiNode that = (QmiNode) o;
        return appId == that.appId &&
                envId == that.envId &&
                typeId == that.typeId &&
                minDuration == that.minDuration &&
                maxDuration == that.maxDuration &&
                tstart == that.tstart &&
                tstop == that.tstop &&
                errorFlag == that.errorFlag &&
                minCalls == that.minCalls &&
                maxCalls == that.maxCalls &&
                minRecs == that.minRecs &&
                maxRecs == that.maxRecs &&
                minErrs == that.minErrs &&
                maxErrs == that.maxErrs &&
                Objects.equals(desc, that.desc) &&
                Objects.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                appId, envId, typeId,
                minDuration, maxDuration, tstart, tstop,
                minCalls, maxCalls, minRecs, maxRecs, minErrs, maxErrs,
                errorFlag, uuid, desc);
    }
}

