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

package io.zorka.tdb.text;

import io.zorka.tdb.ZicoException;
import io.zorka.tdb.meta.MetaIndexUtils;
import io.zorka.tdb.search.EmptySearchResult;
import io.zorka.tdb.search.SearchNode;
import io.zorka.tdb.search.rslt.MappingSearchResult;
import io.zorka.tdb.search.rslt.SearchResult;
import io.zorka.tdb.search.ssn.TextNode;
import io.zorka.tdb.text.AbstractTextIndex;
import io.zorka.tdb.text.RawDictCodec;
import io.zorka.tdb.text.TextIndexUtils;
import io.zorka.tdb.text.WritableTextIndex;
import io.zorka.tdb.util.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.Function;

import static io.zorka.tdb.meta.MetadataTextIndex.FIDS_MARKER;
import static io.zorka.tdb.meta.MetadataTextIndex.TIDS_MARKER;
import static io.zorka.tdb.meta.MetadataTextIndex.TID_MARKER;

/**
 * This is writable index that will store additional items in Write Ahead Log to be later
 * rebuilt as FM index. File format: 'WAL0'[data], where WAL0 is file header, then there
 * is raw data ready to be BWT'd and compressed into FMI file. There might be a bunch of
 * zeros trailing data (as file is memory mapped)
 *
 * When opening file,
 *
 */
public class WalTextIndex extends AbstractTextIndex implements WritableTextIndex {

    /** Default WAL file size limit is 16MB. Bigger files might be better if you have enough memory for BWT transform. */
    public final static int DEFAULT_WAL_SIZE = 16 * 1024 * 1024;

    /** Default quick map size. Quick map allocates 20*size bytes in two blocks.  */
    public final static int DEFAULT_MAP_SIZE = 128 * 1024;

    public final static int WAL_HDR_SIZE = 4;

    private int flimit, fpos;
    private int idBase;
    private int nextId;

    private RandomAccessFile raf;
    private FileChannel channel;
    private MappedByteBuffer buffer;

    private File file;

    private volatile QuickHashTab qmap;

    public WalTextIndex(String path, int idBase) {
        this(path, idBase, DEFAULT_WAL_SIZE);
    }


    public WalTextIndex(String path, int idBase, int flimit) {
        this(new File(path), idBase, flimit, DEFAULT_MAP_SIZE);
    }

    public WalTextIndex(String path, int idBase, int flimit, int mapSize) {
        this(new File(path), idBase, flimit, mapSize);
    }

    public WalTextIndex(File file, int idBase, int flimit) {
        this(file, idBase, flimit, DEFAULT_MAP_SIZE);
    }

    public WalTextIndex(File file, int idBase, int flimit, int mapSize) {
        this.file = file;
        this.flimit = flimit;
        this.idBase = idBase;

        try {
            raf = new RandomAccessFile(file.getPath(), "rw");
            raf.setLength(flimit);
            channel = raf.getChannel();
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, flimit);
            qmap = new QuickHashTab(mapSize);

            if (0 == buffer.get(0)) {
                buffer.put("WAL\03".getBytes(), 0, WAL_HDR_SIZE);

            }
            scan();
        } catch (IOException e) {
            throw new ZicoException("I/O error", e);
        }
    }

    private int hash(byte[] buf, int offs, int len) {
        return (int)(ZicoUtil.crc32(buf, offs, len) & 0x1fffffffL);
    }

    private int hash(ByteBuffer bb, int offs, int len) {
        byte[] buf = new byte[len];
        for (int i = 0; i < len; i++) {
            buf[i] = bb.get(offs+i);
        }
        return hash(buf, 0, buf.length);
    }

    /**
     * Looks up for end of (already written) data.
     */
    private void scan() throws IOException {
        int rstart = WAL_HDR_SIZE, rstop = 0, istart = 0, istop;
        int id ;
        byte[] ibuf = new byte[8];

        nextId = idBase;

        // TODO check header magic number here

        for (int i = WAL_HDR_SIZE; i < flimit; i++) {
            byte b = buffer.get(i);
            if (b == RawDictCodec.MARK_TXT) {
                rstop = i;
                istart = i+1;
            } else if (b == RawDictCodec.MARK_ID2) {
                istop = i;
                buffer.position(istart);
                buffer.get(ibuf, 0, istop - istart);
                id = (int) RawDictCodec.idDecode(ibuf, 0, istop - istart);
                int hash = hash(buffer, rstart, rstop - rstart);
                while (!qmap.put(id - idBase, hash, rstart)) {
                    qmap = qmap.extend();
                }

                nextId = Math.max(id + 1, nextId);
            } else if (b == RawDictCodec.MARK_ID1) {
                rstart = i+1;
            } else if (b == 0) {
                fpos = i;
                break;
            }
        }


        if (fpos > WAL_HDR_SIZE) {
            if (buffer.get(fpos - 1) != RawDictCodec.MARK_ID2) {
                throw new ZicoException("Unexpected end of WAL file");
            }

            for (int pos = fpos - 2; pos > 0; pos--) {
                if (buffer.get(pos) == RawDictCodec.MARK_TXT) {
                    pos++;
                    int l = fpos - pos - 1;
                    byte[] b = new byte[l];
                    for (int i = 0; i < l; i++) {
                        b[i] = buffer.get(pos+i);
                    }
                    if (nextId != RawDictCodec.idDecode(b) + 1) {
                        throw new ZicoException("Last ID mismatch");
                    }
                    return;
                }
            }
            throw new ZicoException("Cannot determine last ID in WAL file.");
        }
    }


    @Override
    public int getNWords() {
        return nextId-idBase;
    }

    @Override
    public long getDatalen() {
        return fpos-WAL_HDR_SIZE;
    }

    synchronized int getFilePos() {
        return fpos;
    }

    synchronized MappedByteBuffer getMappedBuffer() {
        return buffer;
    }

    @Override
    public String getPath() {
        return file.getPath();
    }

    public int getIdBase() {
        return idBase;
    }


    /**
     * Useful only for full text searches. Exact searches are performed using quick map.
     */
    private int lookup(byte[] pbuf, int plen, int start, byte sm, boolean incl, boolean skip) {

        for (int i = start; i < fpos - plen; i++) {
            byte b = buffer.get(i);
            if (b == sm) {
                if (!incl) {
                    i++;
                }
                boolean match = true;
                for (int j = 0; j < plen; j++) {
                    if (pbuf[j] != buffer.get(i+j)) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    return skip ? i + plen : i;
                }
            }
        }
        return -1;
    }


    private byte[] extract(int start, byte em) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i = start; i < fpos; i++) {
            byte b = buffer.get(i);

            if (b != em)
                bos.write(b);
            else
                break;
        }
        return bos.toByteArray();
    }


    @Override
    public byte[] get(int id) {

        int pos = qmap.getById(id-idBase);

        return pos > 0 ? TextIndexUtils.unescape(extract(pos, RawDictCodec.MARK_TXT)) : null;
    }


    /**
     * Returns raw data from index
     * @param term insert record termination character at the beginning;
     * @param term insert terminating character ('\0')
     * @param rev reverse order (record-wise)
     * @return raw WAL data (suitable for BWT transform)
     */
    public synchronized int getData(byte[] rslt, int offs, boolean lead, boolean term, boolean rev) {
        int dl = (int)getDatalen(), ll = lead ? 1 : 0, tl = term ? 1 : 0;

        buffer.position(WAL_HDR_SIZE);

        if (rev) {
            int op = dl+ll, ip = WAL_HDR_SIZE, ep = ip + dl;

            while (ip < ep) {
                int p = ip;
                while (buffer.get(p) != RawDictCodec.MARK_ID2) p++;
                p++;
                int l = p - ip;
                buffer.get(rslt, offs+op-l, l);
                op -= l;
                ip = p;
            }
        } else {
            buffer.get(rslt, offs+ll, dl);
        }

        if (lead)
            rslt[offs] = RawDictCodec.MARK_ID2;

        if (term)
            rslt[offs + dl + ll + tl - 1] = 0;

        return dl;
    }


    public synchronized byte[] getData(boolean lead, boolean term, boolean rev) {
        int dl = (int)getDatalen(), ll = lead ? 1 : 0, tl = term ? 1 : 0;
        byte[] rslt = new byte[ll+dl+tl];

        getData(rslt, 0, lead, term, rev);

        return rslt;
    }


    private boolean chunkEquals(byte[] a, int aoffs, int alen, ByteBuffer b, int boffs) {
        if (aoffs + alen >= fpos) {
            return false;
        }
        for (int i = 0; i < alen; i++) {
            byte x = buffer.get(boffs+i);
            if (x == 0 || x == RawDictCodec.MARK_ID1 || x == RawDictCodec.MARK_ID2
                || x == RawDictCodec.MARK_TXT || a[aoffs+i] != x) {
                return false;
            }
        }
        return b.get(boffs + alen) == RawDictCodec.MARK_TXT;
    }



    @Override
    public int get(byte[] buf, int offs, int len, boolean esc) {

        if (esc) {
            byte[] eb = TextIndexUtils.escape(buf, offs, len);
            if (eb != buf) {
                buf = eb;
                offs = 0;
                len = eb.length;
            }
        }

        int hash = hash(buf, offs, len), idx = 0;

        while (idx < QuickHashTab.MAX_STEPS) {
            long l = qmap.getByHash(hash, idx);
            if (l == -1)
                return -1;
            int pos = (int)(l >>> 32);
            if (chunkEquals(buf, offs, len, buffer, pos)) {
                return (int)RawDictCodec.idDecode(extract(pos + len + 1, RawDictCodec.MARK_ID2));
            }

            idx = (int)(l & 0xffffffffL) + 1;
        }

        return -1;
    }


    @Override
    public long length() {
        return fpos;
    }


    public synchronized void close() throws IOException {
        flush();
        ZicoUtil.unmapBuffer(buffer);
        channel.close();
        raf.close();
        channel = null;
        raf = null;
        buffer = null;
        qmap = null;
    }


    @Override
    public int add(byte[] buf, int offs, int len) {
        return add(buf, offs, len, true);
    }

    public int add(byte[] buf, int offs, int len, boolean esc) {

        if (esc) {
            byte[] eb = TextIndexUtils.escape(buf, offs, len);
            if (eb != buf) {
                buf = eb;
                offs = 0;
                len = eb.length;
            }
        }

        int id = get(buf, offs, len, false);
        if (id != -1) {
            return id;
        }

        synchronized (this) {
            if (-1 != (id = get(buf, offs, len, false))) {
                return id;
            }

            id = nextId++;

            byte[] ib = new byte[8];

            int il = RawDictCodec.idEncode(ib, 0, id);

            if (fpos + 2 * il + len + 3 > flimit) {
                return -1;
            }

            buffer.position(fpos);
            buffer.put(ib, 0, il);
            buffer.put(RawDictCodec.MARK_ID1);
            buffer.put(buf, offs, len);
            buffer.put(RawDictCodec.MARK_TXT);
            buffer.put(ib, 0, il);
            buffer.put(RawDictCodec.MARK_ID2);

            while (!qmap.put((int)(id-idBase), hash(buf, offs, len), fpos + il + 1)) {
                qmap = qmap.extend();
            }

            fpos = buffer.position();

            return id;
        }
    }


    @Override
    public void flush() {
        buffer.force();
    }


    public File getFile() {
        return file;
    }


    public void archive(File newPath) {
        this.file.renameTo(newPath); // TODO handle errors here
        this.file = newPath;
        // TODO some flag here
    }


    @Override
    public String toString() {
        return "WalTextIndex(" + file + ", dlen=" + getDatalen() + ", nwords=" + getNWords() + ")";
    }


    private int jump(int pos, byte b) {
        while (pos < fpos && buffer.get(pos) != b) pos++;
        return pos;
    }

    private int jump(int pos) {
        return jump(pos, RawDictCodec.MARK_ID1);
    }

    private void scan(byte[] text, BitmapSet matches, boolean fetchTids) {
        int rec = 0;
        int limit = fpos - text.length;  // TODO concurrency
        byte[] buf = new byte[8];

        for (int pos = jump(4) + 1; pos <= limit; pos = jump(pos) + 1) {
            if (pos >= limit) break;
            // TODO obsłużenie matchStart i matchEnd
            while (pos < limit) {
                boolean match = true, mbrk = false;
                for (int i = 0 ; i < text.length; i++) {
                    byte b = buffer.get(pos+i);
                    if (b >= 0 && b <= RawDictCodec.MARK_LAST) {
                        match = false; mbrk = true; break;
                    } else if (text[i] != b) {
                        match = false; break;
                    }
                }
                if (mbrk) {
                    break;
                } else if (match) {
                    if (fetchTids) {
                        for (int i = 0; i < 8; i++) {
                            byte b = buffer.get(pos-i-1);
                            if (b >= 0 && b < 32) {
                                if (i > 1) ZicoUtil.reverse(buf, 0, i);
                                matches.add((int)RawDictCodec.idDecode(buf, 0, i));
                                break;
                            } else {
                                buf[i] = b;
                            }
                        }
                    } else {
                        matches.add(rec);
                    }
                    break;
                } else {
                    pos++;
                }
            } // while (pos < limit)
            rec++;
        } // for ()
    }

    @Override
    public SearchResult search(SearchNode expr) {
        if (expr instanceof TextNode) {
            byte[] text = ((TextNode)expr).getText();
            BitmapSet matches = new BitmapSet();

            scan(text, matches, false);

            return new MappingSearchResult(matches.searchAll(), x -> x + idBase);
        } else {
            return EmptySearchResult.INSTANCE;
        }
    }

    @Override
    public SearchResult searchIds(long tid, boolean deep) {
        byte[] text = MetaIndexUtils.encodeMetaInt(TID_MARKER, (int)tid, deep ? FIDS_MARKER : TIDS_MARKER);
        BitmapSet matches = new BitmapSet();

        scan(text, matches, true);

        return new MappingSearchResult(matches.searchAll(), Function.identity());
    }
}


