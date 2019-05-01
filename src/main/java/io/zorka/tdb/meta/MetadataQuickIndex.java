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

package io.zorka.tdb.meta;

import io.zorka.tdb.ZicoException;
import io.zorka.tdb.search.*;
import io.zorka.tdb.util.ZicoMaintObject;
import io.zorka.tdb.util.ZicoUtil;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 * Quick metadata index is useful for searching using standard filters: trace type,
 * duration, application, environment, status, timestamp.
 *
 * It is implemented as a simple memory mapped array that can be iterated quickly.
 * If consists of a header and array of fixed size records. As records are added
 * but not removed, record index is later used as part of chunkId exposed to user
 * code.
 *
 * Header format (64 bytes):
 *
 * 0        8       16       24       32       40       48       56       64
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |  'M'      'Q'      'I'       '0'  |              fpos                 | W1
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |              tstart               |               tstop               | W2
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |                                                                       | W3
 * |                                                                       | W4
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |                                                                       | W5
 * |                          SHA256 sum                                   | W6
 * |                            (archived indexes only)                    | W7
 * |                                                                       | W8
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 *
 *
 * Record format (64 bytes):
 *
 * 0        8       16       24       32       40       48       56       64
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |            tstamp                 |             startOffs             | W1
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |e|tst-ms|  errL  |     typeId      |      envId      |     appId       | W2
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |                          dataOffs                   |     chunkNum    | W3
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |              did                  |    duration     |      hostId     | W4
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |             ftid                  |               ttid                | W5
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |             calls                 |  errH  |     recs                 | W6
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |                                traceId1                               | W7
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |                                traceId2                               | W8
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |                               parentId                                | W9
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |                                spanId                                 | W10
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |                                reserved                               | W11
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * |                                reserved                               | W12
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 *
 * W1 - timestamp and offset
 * tstamp - timestamp (seconds since Epoch);
 * startOffs - logical offset (of chunk inside whole trace)
 *
 * W2 - various trace attributes
 * e - error flag (if 1, tracer has marked error);
 * tst-ms - millisecond part of timestamp (rounded up to 10-s of millisecods);
 * typeId - trace type ID (assigned from configuration database);
 * envId - environment ID (assigned from configuration database);
 * appId - application ID (assigned from configuration database);
 *
 * W3 - trace store position
 * dataOffs - offset in trace data store;
 * chunkNum - chunk sequential number
 *
 * W4 - description and duration
 * did - description text ID (either templated description or method desc);
 * duration - trace (chunk) duration (ticks);
 *
 * W5 - metadata index references
 * ftid - full-depth search metadata
 * ttid - shallow search metadata (only top trace record)
 *
 * W6 - tracer stats, if values greater than
 * calls - number of calls registered by tracer (up to 4G);
 * recs - number of records registered by tracer (up to 24M);
 * errors - number of errors registered by tracer (up to 64k) - errL|errH (lower and higher byte);
 *
 * W7, W8 - trace UUID (encoded)
 *
 */
public class MetadataQuickIndex implements Closeable, ZicoMaintObject {

    private final static int DEFAULT_DELTA = 16 * 1024 * 1024;
    private final static int DEFAULT_FUZZ  = 1024;

    private final static int OFFS_MAGIC = 0;
    private final static int OFFS_FPOS = 4;
    private final static int OFFS_TSTART = 8;
    private final static int OFFS_TSTOP = 12;

    private final static int WORD_SIZE     = 8;
    private final static int RECORD_SIZE   = 12 * WORD_SIZE;
    private final static int HEADER_SIZE   = 8 * WORD_SIZE;


    private final static int W1_OFFS = 0;
    private final static int W2_OFFS = WORD_SIZE;
    private final static int W3_OFFS = 2 * WORD_SIZE;
    private final static int W4_OFFS = 3 * WORD_SIZE;
    private final static int W5_OFFS = 4 * WORD_SIZE;
    private final static int W6_OFFS = 5 * WORD_SIZE;
    private final static int W7_TID1 = 6 * WORD_SIZE;
    private final static int W8_TID2 = 7 * WORD_SIZE;
    private final static int W9_PID  = 8 * WORD_SIZE;
    private final static int W10_SID = 9 * WORD_SIZE;


    private volatile int flimit, delta;
    private volatile int fpos;
    private volatile long tstart, tstop;

    private volatile MappedByteBuffer buffer;

    private final RandomAccessFile raf;
    private final FileChannel channel;

    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    private final Lock mlock = new ReentrantLock();

    private final File file;

    public MetadataQuickIndex(File file) {
        this(file, DEFAULT_DELTA);
    }

    public MetadataQuickIndex(File file, int delta) {
        this.file = file;
        this.delta = delta;

        try {
            raf = new RandomAccessFile(file.getPath(), "rw");
            if (raf.length() > 0) {
                flimit = (int)raf.length();
            } else {
                flimit = delta;
                fpos = HEADER_SIZE;
                raf.setLength(flimit);
            }
            channel = raf.getChannel();
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, flimit);
            if (buffer.get(OFFS_MAGIC) == 0) {
                buffer.put("MQI0".getBytes(), OFFS_MAGIC, 4);
            }
            fpos = buffer.getInt(OFFS_FPOS);
            synchronized (this) {
                tstart = buffer.get(OFFS_TSTART);
                tstop = buffer.get(OFFS_TSTOP);
            }
            if (fpos == 0) {
                fpos = HEADER_SIZE;
            }
        } catch (IOException e) {
            throw new ZicoException("I/O error", e);
        }
    }


    private static long format_W1(int startOffs, long tstamp) {
        return ((long)startOffs) << 32 | tstamp;
    }

    private static void parse_W1(long w1, ChunkMetadata md) {
        md.setTstamp((int)w1);
        md.setStartOffs((int)(w1 >>> 32));
    }

    private static long format_W2(ChunkMetadata md) {
        int errL = md.getErrors() > 0x10000 ? 0xff : (md.getErrors() & 0xff);
        return (((long)md.getAppId()) << 48)
                | (((long)md.getEnvId()) << 32)
                | (((long)md.getTypeId()) << 16)
                | errL << 8
                | md.getFlags();
    }

    private static void parse_W2(long w2, ChunkMetadata md) {
        md.setFlags((int)w2 & 0xff);
        md.setErrors((int)(w2 >> 8) & 0xff);
        md.setTypeId((int)(w2 >> 16) & 0xffff);
        md.setEnvId((int)(w2 >> 32) & 0xffff);
        md.setAppId((int)(w2 >> 48) & 0xffff);
    }

    private static int parse_W2_dur(long w2) {
        return (int)((w2 >> 8) & 0xff);
    }

    private static int parse_W2_flags(long w2) {
        return (int)(w2 & 0xff);
    }

    private static long format_W2M(ChunkMetadata md) {
        return
                (md.getAppId()  != 0 ? 0xffff000000000000L : 0L)
                        | (md.getEnvId()  != 0 ? 0x0000ffff00000000L : 0L)
                        | (md.getTypeId() != 0 ? 0x00000000ffff0000L : 0L)
                        | (md.getFlags()  != 0 ? 0x0000000000000001L : 0L);
    }

    private long format_W2(QmiNode md) {
        return (((long)md.getAppId()) << 48)
                | (((long)md.getEnvId()) << 32)
                | (((long)md.getTypeId()) << 16)
                | (md.isErrorFlag() ? 1 : 0);
    }

    private long format_W2M(QmiNode md) {
        return
                (md.getAppId()  != 0 ? 0xffff000000000000L : 0L)
                        | (md.getEnvId()  != 0 ? 0x0000ffff00000000L : 0L)
                        | (md.getTypeId() != 0 ? 0x00000000ffff0000L : 0L)
                        | (md.isErrorFlag() ? 0x0000000000000001L : 0L);
    }


    private static long format_W3(int chunkNum, long dataOffs) {
        return (((long)chunkNum) << 48) | dataOffs;
    }

    private static void parse_W3(long w3, ChunkMetadata md) {
        md.setDataOffs(w3 & 0xffffffffffffL);
        md.setChunkNum((int)(w3 >>> 48) & 0xffff);
    }

    public static long format_W4(int mid, int did, long duration, long hostId) {
        long md = did > 0 ? ((long) did) | 0x80000000L : mid;
        return md | (Math.min(duration, 0xffff) << 32) | (Math.min(hostId, 0xffff) << 48);
    }


    public static void parse_W4(long w4, ChunkMetadata md) {
        if (0 != (w4 & 0x80000000L)) {
            md.setDescId((int)(w4 & 0x7fffffff));
        } else {
            md.setMethodId((int) (w4 & 0x7fffffff));
        }

        md.setDuration((w4 >>> 32) & 0xffff);
        md.setHostId((int)((w4 >>> 48) & 0xffff));
    }


    public static int parse_W4_did(long w4) {
        return (int)w4 & 0x7fffffff;
    }


    public static long format_W5(int ftid, int ttid) {
        return (((long)ttid) << 32) | ftid;
    }


    public static int parse_W5_ttid(long w5) {
        return (int)(w5 >>> 32);
    }


    public static int parse_W5_ftid(long w5) {
        return (int)w5;
    }


    public static long format_W6(ChunkMetadata md) {
        int errH = md.getErrors() > 0x10000 ? 0xff : (md.getErrors() >> 8);
        int recs = md.getRecs() > 0x1000000 ? 0xffffff : md.getRecs();
        return ((long)md.getCalls()) | (((long)errH) << 32) | (((long)recs) << 40);
    }


    public static void parse_W6(long w6, ChunkMetadata md) {
        int errH = ((int)(w6 >>> 32)) & 0xff;
        md.setCalls((int)w6);
        md.setErrors(md.getErrors() | (errH << 8));
        md.setRecs((int)(w6 >>> 40));
    }


    public synchronized int add(ChunkMetadata md) {
        long w1 = format_W1(md.getStartOffs(), md.getTstamp() / 1000);
        long w2 = format_W2(md);
        long w3 = format_W3(md.getChunkNum(), md.getDataOffs());
        long w4 = format_W4(md.getMethodId(), md.getDescId(), md.getDuration(), md.getHostId());
        long w6 = format_W6(md);

        if (fpos > flimit - RECORD_SIZE) {
            extend();
        }

        rwlock.readLock().lock();

        int rslt;

        try {
            buffer.putLong(fpos + W1_OFFS, w1);
            buffer.putLong(fpos + W2_OFFS, w2);
            buffer.putLong(fpos + W3_OFFS, w3);
            buffer.putLong(fpos + W4_OFFS, w4);
            // w5 will be filled separately
            buffer.putLong(fpos + W6_OFFS, w6);

            buffer.putLong(fpos + W7_TID1, md.getTraceId1());
            buffer.putLong(fpos + W8_TID2, md.getTraceId2());
            buffer.putLong(fpos + W9_PID, md.getParentId());
            buffer.putLong(fpos + W10_SID, md.getSpanId());

            rslt = (fpos - HEADER_SIZE) / RECORD_SIZE;
            fpos += RECORD_SIZE;
            buffer.putInt(OFFS_FPOS, fpos);

            long tst = md.getTstamp();

            if (tstart == 0 || tst < tstart) {
                tstart = tst;
                buffer.putInt(OFFS_TSTART, (int) tst);
            }

            if (tstop == 0 || tst > tstop) {
                tstop = tst;
                buffer.putInt(OFFS_TSTOP, (int) tst);
            }
        } finally {
            rwlock.readLock().unlock();
        }

        return rslt;
    }


    public synchronized void setTids(int slotId, int ftid, int ttid) {
        long w5 = format_W5(ftid, ttid);

        int pos = HEADER_SIZE + slotId * RECORD_SIZE + 4 * WORD_SIZE;

        if (pos >= fpos) {
            throw new ZicoException("Non-existend slotId: " + slotId);
        }

        buffer.putLong(pos, w5);
    }

    public synchronized int getFtid(int slotId) {
        int pos = HEADER_SIZE + slotId * RECORD_SIZE + 4 * WORD_SIZE;

        if (pos >= fpos) {
            throw new ZicoException("Non-existend slotId: " + slotId);
        }

        return parse_W5_ftid(buffer.getLong(pos));
    }

    public synchronized int getTtid(int slotId) {
        int pos = HEADER_SIZE + slotId * RECORD_SIZE + 4 * WORD_SIZE;

        if (pos >= fpos) {
            throw new ZicoException("Non-existend slotId: " + slotId);
        }
        return parse_W5_ttid(buffer.getLong(pos));
    }

    public synchronized int getDid(int slotId) {
        int pos = HEADER_SIZE + slotId * RECORD_SIZE + 3 * WORD_SIZE; // W4

        if (pos >= fpos) {
            throw new ZicoException("Non-existend slotId: " + slotId);
        }

        return parse_W4_did(buffer.getLong(pos));
    }


    public synchronized long getDataOffs(int traceId) {
        int pos = HEADER_SIZE + traceId * RECORD_SIZE + 2 * WORD_SIZE;

        if (pos >= fpos) {
            return -1;
        }

        return buffer.getLong(pos) & 0xffffffffffffL;
    }


    public int getFlags(int slotId) {
        int pos = HEADER_SIZE + slotId * RECORD_SIZE + WORD_SIZE;

        if (pos >= fpos) {
            return -1;
        }

        return parse_W2_flags(buffer.getLong(pos));
    }


    public ChunkMetadata getChunkMetadata(int chunkIdx) {
        int pos = HEADER_SIZE + chunkIdx * RECORD_SIZE;

        if (pos >= fpos) {
            return null;
        }

        ChunkMetadata md = new ChunkMetadata(
                buffer.getLong(pos + W7_TID1),
                buffer.getLong(pos + W8_TID2),
                buffer.getLong(pos + W9_PID),
                buffer.getLong(pos + W10_SID),
                0);

        parse_W1(buffer.getLong(pos), md);
        parse_W2(buffer.getLong(pos+WORD_SIZE), md);
        parse_W3(buffer.getLong(pos+2*WORD_SIZE), md);
        parse_W4(buffer.getLong(pos+3*WORD_SIZE), md);
        // TODO where is w5 ?
        parse_W6(buffer.getLong(pos+5*WORD_SIZE), md);

        return md;
    }


    public void findChunkIds(List<Long> acc, int storeId, long traceId1, long traceId2, long spanId) {
        long mask = (long)(storeId) << 32;
        for (int pos = HEADER_SIZE, i = 0; pos < fpos; pos += RECORD_SIZE, i++) {
            long w7 = buffer.getLong(pos + W7_TID1);
            long w8 = buffer.getLong(pos + W8_TID2);
            long w10 = buffer.getLong(pos + W10_SID);
            if (traceId1 == w7 && traceId2 == w8 &&
                spanId == w10) {
                acc.add(mask | i);
            }
        }
    }

    public long getTraceDuration(int chunkIdx) {
        int pos = HEADER_SIZE + chunkIdx * RECORD_SIZE;

        if (pos >= fpos) {
            return -1;
        }

        return (buffer.getLong(pos + 3 * WORD_SIZE) >>> 32) & 0xffff;
    }

    public long getTstart() {
        return tstart;
    }

    public long getTstop() {
        return tstop;
    }

    private synchronized void extend() {
        // TODO double check here for fpos > flimit - RECORD_SIZE (due to concurrency)
        rwlock.writeLock().lock();
        try {
            raf.setLength(raf.length() + delta);
            ZicoUtil.unmapBuffer(buffer);
            flimit += delta;
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, flimit);
        } catch (IOException e) {
            throw new ZicoException("Cannot extend metaindex file: " + file, e);
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    public synchronized void archive() {
        rwlock.writeLock().lock();
        try {
            ZicoUtil.unmapBuffer(buffer);
            raf.setLength(fpos);
            flimit = fpos;
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, flimit);
        } catch (IOException e) {
            throw new ZicoException("Cannot extend metaindex file: " + file, e);
        } finally {
            rwlock.writeLock().unlock();
        }
    }


    public int size() {
        return (fpos - HEADER_SIZE) / RECORD_SIZE;
    }

    public void flush() {
        buffer.force();
    }

    @Override
    public void close() throws IOException {
        flush();
        ZicoUtil.unmapBuffer(buffer);
        channel.close();
        raf.close();
        buffer = null;
    }

    public int searchBlock(QmiNode query, SortOrder sortOrder, int blkFrom, int blkTo, int[] ids, int[] vals) {
        int idx = 0;
        int pos = HEADER_SIZE + blkFrom * RECORD_SIZE;
        int lim = HEADER_SIZE + blkTo * RECORD_SIZE;
        int blk = blkFrom;
        long t0 = query.getTstart() / 1000;
        long t1 = query.getTstop() / 1000;
        long w2v = format_W2(query);
        long w2m = format_W2M(query);
        long d0 = query.getMinDuration();
        long d1 = query.getMaxDuration();
        long hostId = query.getHostId();
        long tid1 = query.getTraceId1();
        long tid2 = query.getTraceId2();
        long sid = query.getSpanId();
        long pid = query.getParentId();

        ByteBuffer bb = buffer;
        rwlock.readLock().lock();

        try {
            while (pos < lim  && blk-blkFrom < ids.length) {
                long w1 = bb.getLong(pos);
                long w2 = bb.getLong(pos + WORD_SIZE);
                long w4 = bb.getLong(pos + WORD_SIZE * 3);
                long w7 = bb.getLong(pos + W7_TID1);
                long w8 = bb.getLong(pos + W8_TID2);
                long w9 = bb.getLong(pos + W9_PID);
                long w10 = bb.getLong(pos + W10_SID);
                long wd = (w4 >>> 32) & 0xffff;
                long hd = (w4 >>> 48) & 0xffff;
                long t = w1 & 0xffffffffL;
                if (w2v == (w2 & w2m) && t >= t0 && t <= t1 && wd >= d0 && wd < d1 && (hostId == 0 || hostId == hd) &&
                        ((tid1 == 0 && tid2 == 0) || (tid1 == w7 && tid2 == w8)) &&
                        (sid == 0 || sid == w10) && (pid == 0 || pid == w9)) {
                    ids[idx] = blk;
                    switch (sortOrder) {
                        case NONE:
                            vals[idx] = blk;
                            break;
                        case DURATION:
                            vals[idx] = (int)wd;
                            break;
                        case CALLS:
                            vals[idx] = (int) bb.getLong(pos + WORD_SIZE * 5);
                            break;
                        case RECS:
                            vals[idx] = (int) (bb.getLong(pos + WORD_SIZE * 5) >>> 40);
                            break;
                        case ERRORS:
                            vals[idx] = (int)(((w2 >>> 8) & 0xff) | ((bb.getLong(pos + WORD_SIZE * 5) >>> 32) & 0xff));
                            break;
                        default:
                            vals[idx] = blk;
                    }
                    idx++;
                }

                pos += RECORD_SIZE; blk++;
            }
        } finally  {
            rwlock.readLock().unlock();
        }

        return idx;
    }

    @Override
    public boolean runMaintenance() {
        if (mlock.tryLock()) {
            try {
                flush();
            } finally {
                mlock.unlock();
            }
        }
        return false;
    }
}
