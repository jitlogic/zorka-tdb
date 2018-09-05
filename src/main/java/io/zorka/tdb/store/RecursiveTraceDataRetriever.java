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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import static com.jitlogic.zorka.cbor.TraceDataFormat.*;

/**
 * Stateful trace data builder that reconstructs trace call tree and transforms
 * all records using supplied function.
 */
public class RecursiveTraceDataRetriever<T> implements TraceDataRetriever<T> {

    private Stack<TraceRecord> stack = new Stack<>();

    private TraceRecordFilter<T> filter;
    private StructuredTextIndex resolver;

    private TraceDataReader reader;

    private T result;

    /** Depth limit */
    private int stackLimit = Integer.MAX_VALUE;

    public RecursiveTraceDataRetriever(TraceRecordFilter<T> filter) {
        this.filter = filter;
    }

    public void setReader(TraceDataReader reader) {
        this.reader = reader;
    }

    public T getResult() {
        return result;
    }


    public int getStackLimit() {
        return stackLimit;
    }


    public void setStackLimit(int stackLimit) {
        this.stackLimit = stackLimit;
    }


    @Override
    public void traceStart(int pos) {
        stack.push(stack.size() < stackLimit ? new TraceRecord(pos) : null);

    }


    @Override
    public void traceEnd() {
        TraceRecord tr0 = stack.pop();
        if (tr0 == null) return;
        tr0.setDuration(tr0.getTstop()-tr0.getTstart());
        T to = filter.filter(tr0, resolver);
        if (to != null) {
            if (!stack.empty()) {
                TraceRecord tr = stack.peek();
                if (tr.getChildren() == null) {
                    tr.setChildren(new ArrayList<>());
                }
                tr.getChildren().add(to);
            } else {
                if (reader != null) {
                    reader.stop();
                }
                result = to;
            }
        }
    }


    @Override
    public void traceInfo(int k, long v) {
        TraceRecord tr = stack.peek();
        if (tr == null) return;
        switch (k) {
            case TI_CHNUM:
                break;
            case TI_CHOFFS:
                break;
            case TI_CHLEN:
                break;
            case TI_TSTAMP:
                tr.setTstamp(v);
                break;
            case TI_DURATION:
                tr.setDuration(v);
                break;
            case TI_TYPE:
                tr.setType((int)v);
                break;
            case TI_RECS:
                tr.setRecs((int)v);
                break;
            case TI_CALLS:
                tr.setNcalls(v);
                break;
            case TI_ERRORS:
                tr.setErrors(v);
                break;
            case TI_FLAGS:
                tr.setFlags((int)v);
                break;
            case TI_TSTART:
                tr.setTstart(v);
                break;
            case TI_TSTOP:
                tr.setTstop(v);
                break;
            case TI_METHOD:
                tr.setMid((int)v);
                break;
            default:
                throw new ZicoException("Unexpected TraceInfo attribute: " + k);
        }
    }


    @Override
    public void attr(Map<Object, Object> data) {
        TraceRecord tr = stack.peek();
        if (tr == null) return;
        if (tr.getAttrs() == null) {
            tr.setAttrs(new HashMap<>());
        }
        tr.getAttrs().putAll(data);
    }


    @Override
    public void exceptionRef(int ref) {
        TraceRecord tr = stack.peek();
        if (tr != null) {
            tr.setEid(ref);
        }
    }


    @Override
    public void exception(ExceptionData ex) {
        TraceRecord tr = stack.peek();
        if (tr != null) {
            tr.setExceptionData(ex);
        }
    }


    @Override
    public void commit() {
        while (!stack.empty()) {
            traceEnd();
        }
    }

    @Override
    public void setResolver(StructuredTextIndex resolver) {
        this.resolver = resolver;
    }

    @Override
    public void clear() {
        stack.clear();
        result = null;
    }
}
