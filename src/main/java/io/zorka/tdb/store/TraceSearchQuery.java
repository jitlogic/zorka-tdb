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

import java.util.Map;
import java.util.TreeMap;

public class TraceSearchQuery {

    /** Will fetch attributes if set to 1 */
    public static final int FETCH_ATTRS   = 0x01; // Fetch attributes

    /** Will search only error traces if set to 1 */
    public static final int ERRORS_ONLY   = 0x02;

    /** Will return whole distribued traces if set to 0,
     *  only single spans if set to 1 */
    public static final int SPANS_ONLY    = 0x04;

    private int flags = 0;
    private long minTstamp = Long.MIN_VALUE, maxTstamp = Long.MAX_VALUE;
    private long minDuration = 0;

    private Map<String,String> attrMatches = new TreeMap<>();

    public TraceSearchQuery attrMatch(String k, String v) {
        attrMatches.put(k,v);
        return this;
    }

    public int getFlags() {
        return flags;
    }

    public TraceSearchQuery setFlags(int flags) {
        this.flags = flags;
        return this;
    }

    public TraceSearchQuery withErrorsOnly() {
        flags |= ERRORS_ONLY;
        return this;
    }

    public boolean hasErrorsOnly() {
        return 0 != (flags & ERRORS_ONLY);
    }

    public TraceSearchQuery withFetchAttrs() {
        flags |= FETCH_ATTRS;
        return this;
    }

    public TraceSearchQuery withoutFetchAttrs() {
        flags &= ~FETCH_ATTRS;
        return this;
    }

    public boolean hasFetchAttrs() {
        return 0 != (flags & FETCH_ATTRS);
    }

    public TraceSearchQuery withSpansOnly() {
        flags |= SPANS_ONLY;
        return this;
    }

    public TraceSearchQuery withoutSpansOnly() {
        flags &= ~SPANS_ONLY;
        return this;
    }

    public boolean hasSpansOnly() {
        return 0 != (flags & SPANS_ONLY);
    }

    public Map<String, String> getAttrMatches() {
        return attrMatches;
    }

    public TraceSearchQuery withAttrMatch(String key, String val) {
        if (attrMatches == null) attrMatches = new TreeMap<>();
        if (key != null && val != null) attrMatches.put(key, val);
        return this;
    }

    public TraceSearchQuery setAttrMatches(Map<String, String> attrMatches) {
        this.attrMatches = attrMatches;
        return this;
    }

    public long getMinTstamp() {
        return minTstamp;
    }

    public TraceSearchQuery setMinTstamp(long minTstamp) {
        this.minTstamp = minTstamp;
        return this;
    }

    public long getMaxTstamp() {
        return maxTstamp;
    }

    public TraceSearchQuery setMaxTstamp(long maxTstamp) {
        this.maxTstamp = maxTstamp;
        return this;
    }

    public long getMinDuration() {
        return minDuration;
    }

    public TraceSearchQuery setMinDuration(long minDuration) {
        this.minDuration = minDuration;
        return this;
    }
}
