/**
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

package io.zorka.tdb.meta;

/**
 * Metadata search query
 */
public class MetadataSearchQuery implements MetadataInfo {

    /** Searches whole subtree if set. */
    public final static int DEEP_SEARCH = 0x01;

    /** Default sort index: by index, backwards. */
    public final static int SORT_DEFAULT = 0;

    /** Sort by timestamp, descending order (newest results first). */
    public final static int SORT_TSTAMP = 1;

    /** Sort by duration, descending order (longest executing traces first). */
    public final static int SORT_DURATION = 2;

    /** Application ID (0 to skip search by appId) */
    private int appId;

    /** Environment ID (0 to skip search by envId) */
    private int envId;

    /** Trace type */
    private int typeId;

    /** Trace duration (milliseconds) */
    private long duration;

    /** Trace flags */
    private int tflags;

    /** Starting timestamp */
    private long tstart = 0;

    /** Finishing timestamp */
    private long tstop = Long.MAX_VALUE;

    /** Sort order */
    private int sortOrder;

    /** Sort window size */
    private int windowSize = 1024 * 1024;

    private int calls, errors, recs;

    @Override
    public int getAppId() {
        return appId;
    }

    public void setAppId(int appId) {
        this.appId = appId;
    }

    @Override
    public int getEnvId() {
        return envId;
    }

    public void setEnvId(int envId) {
        this.envId = envId;
    }

    @Override
    public int getTypeId() {
        return typeId;
    }

    public void setTypeId(int typeId) {
        this.typeId = typeId;
    }

    @Override
    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    @Override
    public int getFlags() {
        return tflags;
    }

    public void setTFlags(int flags) {
        this.tflags = flags;
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

    public int getTflags() {
        return tflags;
    }

    public void setTflags(int tflags) {
        this.tflags = tflags;
    }


    public int getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(int sortOrder) {
        this.sortOrder = sortOrder;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(int windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public int getCalls() {
        return calls;
    }

    public void setCalls(int calls) {
        this.calls = calls;
    }

    @Override
    public int getErrors() {
        return errors;
    }

    public void setErrors(int errors) {
        this.errors = errors;
    }

    @Override
    public int getRecs() {
        return recs;
    }

    public void setRecs(int recs) {
        this.recs = recs;
    }
}
