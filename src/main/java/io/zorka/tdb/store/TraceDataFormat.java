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

/**
 *
 */
public class TraceDataFormat {

    // Single-byte encodedd tags (potentially often used).

    public static final int TAG_STRING_DEF = 0x01;
    public static final int TAG_METHOD_DEF = 0x02;
    public static final int TAG_AGENT_ATTR = 0x03;

    /** String reference is a tagged number. */
    public static final int TAG_STRING_REF     = 0x06;

    /** Trace record start */
    public static final int TAG_TRACE_START    = 0x08;

    /** Trace attribute */
    public static final int TAG_TRACE_ATTR     = 0x09;

    public static final int TAG_PROLOG_BE = 0x0a;
    public static final int TAG_PROLOG_LE = 0x0b;
    public static final int TAG_EPILOG_BE = 0x0c;
    public static final int TAG_EPILOG_LE = 0x0d;

    public static final int TAG_TRACE_INFO  = 0x0e;

    public static final int TAG_PROLOG_RAW      = 0x0e;
    public static final int TAG_EPILOG_RAW      = 0x0f;

    // Two-byte encoded tags (less often used ones)
    public static final int TAG_TRACE_BEGIN    = 0x21; /** Trace begin marker */
    public static final int TAG_EXCEPTION      = 0x22;
    public static final int TAG_EXCEPTION_REF  = 0x23;
    public static final int TAG_KEYWORD        = 0x24;
    public static final int TAG_FLAG_TOKEN     = 0x25;
    public static final int TAG_TRACE_UP_ATTR  = 0x26;

    /** Sets error flag */
    public static final int FLAG_ERROR    = 0x01;

    /** Clears error flag */
    public static final int FLAG_NO_ERROR = 0x02;

    /** Chunk number (first chunk has number 0) - suitable only for chunked traces. */
    public static final int TI_CHNUM    = 0x02;

    /** Offset (in bytes) of current chunk in trace data. */
    public static final int TI_CHOFFS   = 0x03;

    /** Chunk length (in bytes). */
    public static final int TI_CHLEN    = 0x04;

    /** Timestamp of the beginning of trace. */
    public static final int TI_TSTAMP   = 0x05;

    /** Trace duration. */
    public static final int TI_DURATION = 0x06;  // Indexable

    /** Trace type (refers to string from text index). */
    public static final int TI_TYPE     = 0x07;  // Translatable, indexable

    /** Number of trace records below current one. */
    public static final int TI_RECS     = 0x08;

    /** Number of method calles registered by tracer in subtree starting in current record. */
    public static final int TI_CALLS    = 0x09;

    /** Number of errors registered by tracer in current subtree (sdtarting with current method). */
    public static final int TI_ERRORS   = 0x0a;

    /** Trace flags */
    public static final int TI_FLAGS    = 0x0b;  // Translatable, indexable

    /** Beginning timestamp. */
    public static final int TI_TSTART   = 0x0c;

    /** Ending timestamp. */
    public static final int TI_TSTOP    = 0x0d;

    /** Method ID. */
    public static final int TI_METHOD   = 0x0e;  // Translatable

    public static final int TI_SKIP     = 0x0f;

    public static final int TI_FLAGS_C  = 0x10;

    /** Marks trace as ending with error (th. thrown exception). */
    public static final int TF_ERROR    = 0x01; // Error flag

    //
    public static final int TRACE_DROP_TOKEN   = 0xe0; /* TRACE DROP is encoded as simple value. */

    /** This is pre-computed 4-byte trace record header. */

    public static final int TREC_HEADER_BE = 0xd80a9f48;
    public static final int TREC_HEADER_LE = 0x489f0bd8;

    public static final byte STRING_TYPE  = 0x00; // Generic string, raw encoding (no prefix);

    public static final byte TYPE_MIN     = 0x04;
    public static final byte KEYWORD_TYPE = 0x04; // LISP keyword:     0x04|keyword_name|0x04
    public static final byte CLASS_TYPE   = 0x05; // Class name        0x05|class_name|0x05
    public static final byte METHOD_TYPE  = 0x06; // Method name       0x06|method_name|0x06
    public static final byte UUID_TYPE    = 0x07; // UUID              0x07|uuid_encoded|0x07
    public static final byte SIGN_TYPE    = 0x08; // Method signature  0x08|method_signature|0x08
    public static final byte TYPE_MAX     = 0x08;

    public static int TICKS_IN_SECOND = 15259;    //
}
