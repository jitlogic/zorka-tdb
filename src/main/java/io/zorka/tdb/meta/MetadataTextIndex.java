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

import io.zorka.tdb.text.RawDictCodec;
import io.zorka.tdb.util.IntegerSeqResult;
import io.zorka.tdb.text.WritableTextIndex;
import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.text.re.SearchPattern;

import static io.zorka.tdb.text.RawDictCodec.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MetadataTextIndex {

    /** TraceID marker. */
    public static final byte TID_MARKER = 0x05;

    /** Ids for top level components */
    public static final byte TIDS_MARKER = 0x06;

    /** IDs for all components */
    public static final byte FIDS_MARKER = 0x07;

    /** TraceID in chunk index */
    public static final byte CHT_MARKER = 0x0b;

    /** Chunk offset */
    public static final byte CHO_MARKER = 0x0d;

    /** Duration flags. */
    private final static long[] DURATIONS = {
        0,      // '0' - sentinel
        1,      // '1' - 1 second
        5,      // '2' - 5 seconds
        15,     // '3' - 15 seconds
        60,     // '4' - 1 minute
        300,    // '5' - 5 minutes
        900,    // '6' - 15 minutes
        3600,   // '7' - 1 hour
        14400,  // '8' - 4 hours
        86400,  // '9' - 1 day
    };

    public static int formatDuration(long duration) {
        long d = duration / 1000;
        for (int i = DURATIONS.length-1; i >= 0; i--) {
            if (d >= DURATIONS[i]) {
                return i;
            }
        }
        return 0;
    }

    /** Error flag. */
    private final static byte ERR_FLAG = (byte)'!';

    /** Duration flags start with '0' and end with '9'. */
    private final static byte ZERO = (byte)'0';

    /** Backing index */
    private WritableTextIndex tidx;

    /** Quick index */
    private MetadataQuickIndex qidx;

    /** Determines how often trace ID should be interleaved with string IDs. */
    private int idTagBits = 4;

    public MetadataTextIndex(WritableTextIndex tidx) {
        this.tidx = tidx;
    }


    public int addTextMetaData(int traceId, List<Integer> tids, boolean deep) {
        byte marker = deep ? FIDS_MARKER : TIDS_MARKER;
        byte[] buf = new byte[32 + 18 * tids.size()];
        int pos = 1;

        buf[0] = marker;

        if (tids.size() == 0) {
            return -1;
        }

        for (Integer id : tids) {
            pos += idEncode(buf, pos, traceId);
            buf[pos++] = TID_MARKER;
            pos += idEncode(buf, pos, id);
            buf[pos++] = marker;
        }

        return tidx.add(buf, 0, pos);
    }


    public int addTraceChunkDesc(int traceId, String traceUUID) {
        byte[] buf = new byte[64];
        int pos = 0;

        pos += idEncode(buf, pos, traceId);
        buf[pos++] = CHO_MARKER;
        System.arraycopy(traceUUID.getBytes(), 0, buf, pos, traceUUID.length());
        pos += traceUUID.length();
        buf[pos++] = CHT_MARKER;

        return tidx.add(buf, 0, pos);
    }


    public List<Integer> findTraceSlots(String traceUUID) {
        byte[] buf = MetaIndexUtils.encodeMetaStr(CHO_MARKER, traceUUID, CHT_MARKER);
        return extractTids(SearchPattern.escape(buf, 0, buf.length), CHO_MARKER);
    }


    public IntegerSeqResult findByIds(int tid, boolean deep) {
        byte[] buf = MetaIndexUtils.encodeMetaInt(TID_MARKER, tid, deep ? FIDS_MARKER : TIDS_MARKER);
        return tidx.searchXIB(buf, TID_MARKER);
    }


    public List<Integer> extractTids(String re, byte marker) {
        List<Integer> rslt = new ArrayList<>();

        IntegerSeqResult sr = tidx.searchIds(re);
        for (int id = sr.getNext(); id != -1; id = sr.getNext()) {
            byte[] b = tidx.get(id);
            for (int i = 0; i < b.length; i++) {
                if (b[i] == marker) {
                    rslt.add((int) RawDictCodec.idDecode(b, 0, i));
                    break;
                }
            }
        }
        return rslt;
    }

    /**
     * Extracts ids of all strings referenced in given chunk.
     * @param id chunk ID
     * @return array of ids of records from text index
     */
    public int[] extractMetaTids(int id) {
        byte[] buf = tidx.get(id);
        if (buf == null) return new int[0];
        int cnt = 0;
        for (byte b : buf) {
            if (b == TID_MARKER) cnt++;
        }
        int[] rslt = new int[cnt];
        int p1 = 0;
        for (int i = 0; i < rslt.length; i++) {
            while (buf[p1] != TID_MARKER) p1++;
            int p2 = p1+1;
            while (buf[p2] != FIDS_MARKER && buf[p2] != TIDS_MARKER) p2++;
            rslt[i] = (int)RawDictCodec.idDecode(buf, p1+1, p2-p1-1);
            p1 = p2;
        }
        return rslt;
    }

    public String getTraceUUID(int traceId) {
        byte[] buf = new byte[16];
        int pos = 0;

        pos += idEncode(buf, pos, traceId);
        buf[pos++] = CHO_MARKER;

        String re = "^" + SearchPattern.escape(buf, 0, pos);

        int id = -1;

        IntegerSeqResult sr = tidx.searchIds(re);
        for (int i = sr.getNext(); i != -1; i = sr.getNext()) {
            // TODO what about multiple results ?
            id = i;
        }

        if (id != -1) {
            byte[] b = tidx.get(id);

            for (int i = 0; i < b.length; i++) {
                if (b[i] == CHO_MARKER) {
                    byte[] rslt = new byte[b.length - i - 2];
                    System.arraycopy(b, i+1, rslt, 0, b.length - i - 2);
                    return new String(rslt);
                }
            }
        }

        return null;
    }


    public void close() throws IOException {
        tidx.close();
        tidx = null;
    }


    public void flush() {
        tidx.flush();
    }

}
