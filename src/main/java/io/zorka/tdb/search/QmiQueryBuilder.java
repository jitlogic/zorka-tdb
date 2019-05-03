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

package io.zorka.tdb.search;

public class QmiQueryBuilder {

    private QmiNode qmi = new QmiNode();

    public static QmiQueryBuilder all() {
        return new QmiQueryBuilder();
    }

    public QmiQueryBuilder minDuration(long min) {
        qmi.setMinDuration(min);
        return this;
    }

    public QmiQueryBuilder duration(long min, long max) {
        qmi.setMinDuration(min);
        qmi.setMaxDuration(max);
        return this;
    }

    public QmiQueryBuilder tstart(long tstart) {
        qmi.setTstart(tstart);
        return this;
    }

    public QmiQueryBuilder tstop(long tstop) {
        qmi.setTstop(tstop);
        return this;
    }

    public QmiQueryBuilder timespan(long tstart, long tstop) {
        qmi.setTstart(tstart);
        qmi.setTstop(tstop);
        return this;
    }

    public QmiQueryBuilder error(boolean errorFlag) {
        qmi.setErrorFlag(errorFlag);
        return this;
    }

    public QmiQueryBuilder minCalls(long minCalls) {
        qmi.setMinCalls(minCalls);
        return this;
    }

    public QmiQueryBuilder calls(long minCalls, long maxCalls) {
        qmi.setMinCalls(minCalls);
        qmi.setMaxDuration(maxCalls);
        return this;
    }

    public QmiQueryBuilder minRecs(long minRecs) {
        qmi.setMinRecs(minRecs);
        return this;
    }

    public QmiQueryBuilder recs(long minRecs, long maxRecs) {
        qmi.setMinRecs(minRecs);
        qmi.setMaxRecs(maxRecs);
        return this;
    }

    public QmiQueryBuilder minErrors(long minErrors) {
        qmi.setMinErrs(minErrors);
        return this;
    }

    public QmiQueryBuilder errors(long minErrors, long maxErrors) {
        qmi.setMinErrs(minErrors);
        qmi.setMaxRecs(maxErrors);
        return this;
    }

    public QmiQueryBuilder traceId(long traceId1, long traceId2) {
        qmi.setTraceId1(traceId1);
        qmi.setTraceId2(traceId2);
        return this;
    }

    public QmiQueryBuilder parentId(long parentId) {
        qmi.setParentId(parentId);
        return this;
    }

    public QmiQueryBuilder spanId(long spanId) {
        qmi.setSpanId(spanId);
        return this;
    }

    public QmiNode qmiNode() {
        return qmi;
    }

    public TraceSearchQuery query() {
        return QueryBuilder.all().query(qmi);
    }
}
