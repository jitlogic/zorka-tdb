/*
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

package io.zorka.tdb.store;

import io.zorka.tdb.ZicoException;
import io.zorka.tdb.util.CBOR;
import io.zorka.tdb.util.CborDataReader;
import io.zorka.tdb.util.Debug;
import io.zorka.tdb.ZicoException;
import io.zorka.tdb.util.CborDataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.zorka.tdb.util.CBOR.*;
import static io.zorka.tdb.store.TraceDataFormat.*;
import static io.zorka.tdb.util.Debug.*;

/**
 *
 */
public class TraceDataReader implements Runnable {

    private final static Logger log = LoggerFactory.getLogger(TraceDataReader.class);

    private CborDataReader reader;
    private StatelessDataProcessor output;

    private boolean running = true;

    // TODO use CborDataReader instead of implementing things by hand

    public TraceDataReader(CborDataReader reader, StatelessDataProcessor output) {
        this.reader = reader;
        this.output = output;
    }

    private void checked(boolean cond) {
        if (!cond) {
            throw new ZicoException("Unexpected end of data.");
        }
    }


    private void checked(boolean cond, String msg) {
        if (!cond) {
            throw new ZicoException(msg);
        }
    }

    public boolean isRunning() {
        return running;
    }

    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        while (running && reader.size() - reader.position() > 0) {
            process();
        }
    }



    private Object read() {
        int peek = reader.peek(), type = peek & TYPE_MASK;

        switch (type) {
            case UINT_BASE:
            case NINT_BASE: {
                return reader.readLong();
            }
            case BYTES_BASE: {
                return reader.readBytes();
            }
            case STR_BASE: {
                return reader.readStr();
            }
            case ARR_BASE: {
                if (peek == ARR_VCODE) {
                    List<Object> lst = new ArrayList<>();
                    reader.read();
                    while (reader.peek() != BREAK_CODE) {
                        lst.add(read());
                    }
                    return lst;
                } else {
                    int len = reader.readInt();
                    List<Object> lst = new ArrayList<>(len);
                    for (int i = 0; i < len; i++) {
                        lst.add(read());
                    }
                    return lst;
                }
            }
            case MAP_BASE: {
                Map<Object,Object> m = new HashMap<>();
                if (peek == MAP_VCODE) {
                    reader.read();
                    while (reader.peek() != BREAK_CODE) {
                        m.put(read(), read());
                    }
                } else {
                    int len = reader.readInt();
                    for (int i = 0; i < len; i++) {
                        m.put(read(), read());
                    }
                }
                return m;
            }
            case TAG_BASE: {
                switch (peek) {
                    case TAG_BASE+ TraceDataFormat.TAG_STRING_REF:
                        reader.readInt();
                        return new ObjectRef(reader.readInt());
                    default:
                        throw new ZicoException("Invalid tag for custom data: " + peek);

                }
            }
        }
        throw new ZicoException("Type " + type + " not allowed in custom data.");
    }


    private void process() {
        int peek = reader.peek();
        int type = peek & TYPE_MASK;

        if (type != TAG_BASE) {
            if (peek == BREAK_CODE) {
                reader.read();
                output.traceEnd(); // TODO this is ambiguous, we should really have some distinct marker for trace end
                return;
            } else {
                throw new ZicoException("Invalid datatype (expected tagged data): " + type);
            }
        }

        int tag = reader.readInt();
        if (TRACE_ENABLED) Debug.trace("TraceDataReader","process(): TAG=" + tag);
        switch (tag) {
            case TraceDataFormat.TAG_TRACE_START: {
                checked(reader.peek() == CBOR.ARR_VCODE,
                    "Trace record should be encoded as unbounded array.");
                int pos = reader.position();
                reader.read();
                output.traceStart(pos - 1);
                break;
            }
            case TraceDataFormat.TAG_PROLOG_BE:
            case TraceDataFormat.TAG_PROLOG_LE: {
                checked(reader.peek() == (CBOR.BYTES_BASE + 8),
                    "Invalid trace record prolog.|");
                reader.read();
                long v = reader.readRawLong(tag == TraceDataFormat.TAG_PROLOG_LE);

                long tstart = v & 0x000000FFFFFFFFFFL;
                int methodId = (int) (v >>> 40);
                output.traceInfo(TraceDataFormat.TI_TSTART, tstart);
                output.traceInfo(TraceDataFormat.TI_METHOD, methodId);
                // TODO immediately check for TRACE_BEGIN tag here
                // TODO ewentualnie przejść na bardziej efektywny TRACE_PROLOG i TRACE_EPILOG aby nie było 2xTRACE_INFO
                break;
            }
            case TraceDataFormat.TAG_EPILOG_BE:
            case TraceDataFormat.TAG_EPILOG_LE: {
                int xf = reader.peek();
                checked(xf == (CBOR.BYTES_BASE + 8) || xf == (CBOR.BYTES_BASE + 16),
                    "Invalid trace epilog.");
                reader.read();
                long v = reader.readRawLong(tag == TraceDataFormat.TAG_EPILOG_LE);
                long tstop = v & 0x000000FFFFFFFFFFL;
                if (xf == 0x48) {
                    int calls = (int) (v >>> 40);
                    output.traceInfo(TraceDataFormat.TI_TSTOP, tstop);
                    output.traceInfo(TraceDataFormat.TI_CALLS, calls);
                } else {
                    long calls = reader.readRawLong(tag == TraceDataFormat.TAG_EPILOG_LE);
                    output.traceInfo(TraceDataFormat.TI_TSTOP, tstop);
                    output.traceInfo(TraceDataFormat.TI_CALLS, calls);
                }
                output.traceEnd();
                if (reader.peek() != CBOR.BREAK_CODE) {
                    throw new ZicoException("Epilog should be last element of trace record.");
                }
                reader.read();
                break;
            }
            case TraceDataFormat.TAG_TRACE_ATTR: {
                Object attrs = read();
                output.attr((Map)attrs);
                break;
            }
            case TraceDataFormat.TAG_TRACE_BEGIN: {
                checked(reader.peek() == 0x82,
                    "Trace begin marker should be 2-element array.");
                reader.read();
                long tstamp = reader.readLong();
                int tid = reader.readInt();
                output.traceInfo(TraceDataFormat.TI_TSTAMP, tstamp);
                output.traceInfo(TraceDataFormat.TI_TYPE, tid);
                break;
            }
            case TraceDataFormat.TAG_EXCEPTION: {
                // TODO handle also stored exception format (with refs), not only wire format
                checked(reader.peek() == 0x85,
                    "Exception description should be 5-element array.");
                reader.read();
                int excId = reader.readInt();
                checked(reader.peek() == TAG_BASE + TraceDataFormat.TAG_STRING_REF,
                    "Expected exception class (as string ref).");
                reader.read();
                int excClass = reader.readInt();
                String msg = null;
                int msgId = 0;

                if (reader.peekType() == STR_BASE) {
                    msg = reader.readStr();  // TODO handle refs here
                } else if (reader.peek() == NULL_CODE) {
                    msg = "";
                    reader.read();
                } else {
                    msgId = reader.readInt();
                }

                int causeId = reader.readInt();

                ExceptionData ex = msg != null
                    ? new ExceptionData(excId, excClass, msg, causeId)
                    : new ExceptionData(excId, excClass, msgId, causeId);

                checked(ARR_BASE == reader.peekType(),
                    "Expected stack trace (sized array).");
                int stackSize = reader.readInt();
                for (int i = 0; i < stackSize; i++) {
                    checked(reader.peek() == ARR_BASE + 4,
                        "Expected stack trace element (4-element item).");
                    reader.read();
                    int sClass = reader.readInt();
                    int sMethod = reader.readInt();
                    int sFile = reader.readInt();
                    int sLine = reader.readInt();
                    ex.stackTrace.add(new StackData(sClass, sMethod, sFile, sLine));
                }
                output.exception(ex);
                break;
            }
            case TraceDataFormat.TAG_EXCEPTION_REF:
                output.exceptionRef(reader.readInt());
                break;
            case TraceDataFormat.TAG_TRACE_INFO: {
                checked(reader.peekType() == MAP_BASE,
                    "Expected map element as TRACE_INFO.");
                int sz = reader.readInt();
                checked(sz >= 1,
                    "Map must be of reasonable size.");
                for (int i = 0; i < sz; i++) {
                    output.traceInfo(reader.readInt(), reader.readLong());
                }
                break;
            }
            case TraceDataFormat.TAG_FLAG_TOKEN:
                int flag = reader.readInt();
                if (flag == TraceDataFormat.FLAG_ERROR) {
                    output.traceInfo(TraceDataFormat.TI_FLAGS, TraceDataFormat.TF_ERROR);
                } else if (flag == TraceDataFormat.FLAG_NO_ERROR) {
                    output.traceInfo(TraceDataFormat.TI_FLAGS_C, TraceDataFormat.TF_ERROR);
                }
                break;
            default:
                throw new ZicoException("Invalid tag: " + tag);
        } // switch (tag)

    } // process()


}
