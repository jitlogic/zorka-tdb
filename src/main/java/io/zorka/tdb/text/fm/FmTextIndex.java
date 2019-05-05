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

package io.zorka.tdb.text.fm;

import io.zorka.tdb.text.AbstractTextIndex;
import io.zorka.tdb.text.RawDictCodec;
import io.zorka.tdb.text.TextIndexState;
import io.zorka.tdb.text.TextIndexUtils;

import io.zorka.tdb.util.BitmapSet;
import io.zorka.tdb.util.ZicoUtil;

import java.io.*;


/**
 * Represents whole FM index. This is
 */
public class FmTextIndex extends AbstractTextIndex {

    private FmIndexFileStore fif;

    public FmTextIndex(File f) {
        this.fif = new FmIndexFileStore(f.getPath(), FmIndexFileStore.CHECK_ALL_SUMS);
    }

    public FmTextIndex(FmIndexFileStore fif) {
        this.fif = fif;
    }

    public FmIndexFileStore getFileStore() {
        return fif;
    }

    @Override
    public String getPath() {
        return fif.getFile().getPath();
    }

    @Override
    public int getIdBase() {
        return fif.getIdBase();
    }

    @Override
    public int getNWords() {
        return fif.getNWords();
    }

    @Override
    public long getDatalen() {
        return fif.getDatalen();
    }

    @Override
    public byte[] get(int id) {
        byte[] rslt;
        lock.readLock().lock();
        try {
            if (getState() == TextIndexState.OPEN) {
                long range = locateById(id);
                rslt = range != -1 ? TextIndexUtils.unescape(extractUntil(sp(range), RawDictCodec.MARK_ID1)) : null;
            } else {
                rslt = null;
            }
        } finally {
            lock.readLock().unlock();
        }
        return rslt;
    }

    @Override
    public int get(byte[] buf, int offs, int len, boolean esc) {
        int rslt;
        lock.readLock().lock();
        try {
            if (getState() == TextIndexState.OPEN) {
                rslt = getInternal(buf, offs, len, esc);
            } else {
                rslt = -1;
            }
        } finally {
            lock.readLock().unlock();
        }
        return rslt;
    }


    private int getInternal (byte[] buf, int offs, int len, boolean esc) {

        if (esc) {
            byte[] eb = TextIndexUtils.escape(buf, offs, len);
            if (eb != null) {
                buf = eb;
                offs = 0;
                len = eb.length;
            }
        }

        byte[] pbuf = new byte[len + 2];  // TODO make this thing GC-free
        pbuf[0] = RawDictCodec.MARK_ID1;
        System.arraycopy(buf, offs, pbuf, 1, len);
        pbuf[pbuf.length-1] = RawDictCodec.MARK_TXT;
        ZicoUtil.reverse(pbuf);
        long range = locateL(pbuf);
        if (range == -1) {
            return -1;
        }
        int sp = ep(range), ep = ep(range);
        int rslt = -1;
        if (sp > ep) {
            return rslt;
        }
        byte[] ibuf = extractUntil(sp, RawDictCodec.MARK_ID2);
        for (int i = 0; i < ibuf.length; i++) {
            if (0 == ibuf[i]) {
                rslt = (int)RawDictCodec.idDecode(ibuf, 0, i-1);
            }
        }
        if (rslt == -1) {
            rslt = (int)RawDictCodec.idDecode(ibuf);
        }
        return rslt;
    }


    @Override
    public long length() {
        return fif.length();
    }


    private byte[] extractUntil(int s0, byte term) {
        int pos = s0;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i = 0; i < fif.getDatalen(); i++) {
            long car = fif.charAndRank(pos);
            byte ch = FmIndexStore.chr(car);
            if (ch == term) break;
            bos.write(ch & 0xff);
            pos = fif.getCharOffs(ch) + FmIndexStore.rnk(car);
        }
        byte[] rslt = bos.toByteArray();
        ZicoUtil.reverse(rslt);
        return rslt;
    }


    private int extractChunk(byte[] buf, int pos) {
        return extractChunk(buf, 0, buf.length, pos);
    }


    private int extractChunk(byte[] buf, int offs, int len, int pos) {
        int i;
        for (i = 0; i < len; i++) {
            long car = fif.charAndRank(pos);
            byte ch = FmIndexStore.chr(car);
            if (ch >= 0 && ch < 32) break;
            buf[offs + i] = ch;
            pos = fif.getCharOffs(ch) + FmIndexStore.rnk(car);
        }
        if (i > 1) ZicoUtil.reverse(buf, 0, i);
        return i;
    }


    private int extractId(int pos) {
        byte[] ibuf = new byte[8];
        int ilen = extractChunk(ibuf, pos);
        return (int)RawDictCodec.idDecode(ibuf, 0, ilen);
    }

    /**
     * Skips one character of original string in backward traversal through BWT string.
     * @param pos initial position
     * @return logical position in raw (non-BWT) string that directly preceeds {@param pos}.
     */
    private int skip(int pos) {
        long car = fif.charAndRank(pos);
        return fif.getCharOffs(FmIndexStore.chr(car)) + FmIndexStore.rnk(car);
    }

    /**
     * Skips n characters of original string in backward traversal through BWT string.
     * @param pos initial BWT position
     * @param n number of characters to skip
     * @return BWT position after skipping n characters
     */
    private int skip(int pos, int n) {
        for (int i = 0; i < n; i++) {
            pos = skip(pos);
        }
        return pos;
    }


    private int skipUntil(int s0, byte b, boolean breakOnCtl) {
        int pos = s0;
        for (int i = 0; i < fif.getDatalen(); i++) {
            long car = fif.charAndRank(pos);
            byte ch = FmIndexStore.chr(car);
            if (ch == b) return pos;
            if (breakOnCtl && (ch & 0xff) <= RawDictCodec.MARK_LAST) return -1;
            pos = fif.getCharOffs(FmIndexStore.chr(car)) + FmIndexStore.rnk(car);
        }
        return -1;
    }

    private long se(long sp, long ep) {
        return sp | (ep << 32);
    }

    private int sp(long se) {
        return (int)(se & 0xffffffffL);
    }

    private int ep(long se) {
        return (int)(se >>> 32);
    }


    /**
     * Performs preliminary search in compressed index. Returns eptr and sptr - indexes in compressed BWT string marking
     * beginning and end of result range.
     * @param pbuf search phrase as byte array in reversed order
     * @return long int containing both eptr and sptr (use sp() and ep() functions to decode both numbers);
     */
    private long locateL(byte[] pbuf) {
        return locateL(pbuf, 0, pbuf.length, 0, fif.getDatalen());
    }


    private long locateL(byte[] pbuf, int poffs, int plen, int s0, int e0) {
        int sp = s0, ep = e0;

        for (int i = 0; i < plen; i++) {
            byte c = pbuf[poffs+i];
            int coffs = fif.getCharOffs(pbuf[i]);
            sp = coffs + fif.rankOf(sp, c);
            ep = coffs + fif.rankOf(ep+1, c) - 1;
            if (sp > ep) {
                return -1L;
            }
        }

        return se(sp, ep);
    }


    private long locateById(int id) {
        byte[] pbuf = new byte[RawDictCodec.idLen(id)+2];
        pbuf[0] = RawDictCodec.MARK_TXT;
        RawDictCodec.idEncode(pbuf, 1, id);
        pbuf[pbuf.length-1] = RawDictCodec.MARK_ID2;

        ZicoUtil.reverse(pbuf);

        return locateL(pbuf);
    }


    public int getPidx() {
        return fif.getPidx();
    }

    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            fif.close();
            setState(TextIndexState.CLOSED);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int search(String text, boolean matchStart, boolean matchEnd, BitmapSet rslt) {
        int r;
        lock.readLock().lock();
        try {
            if (getState() == TextIndexState.OPEN) {
                r = searchInternal(text, matchStart, matchEnd, rslt);
            } else {
                r = 0;
            }
        } finally {
            lock.readLock().unlock();
        }
        return r;
    }

    private int searchInternal(String text, boolean matchStart, boolean matchEnd, BitmapSet rslt) {

        byte[] s = text.getBytes();

        // Prepare search phrase
        byte[] buf = new byte[s.length + (matchStart ? 1 : 0) + (matchEnd ? 1 : 0)];
        System.arraycopy(s, 0, buf, matchStart ? 1 : 0, s.length);

        if (matchStart) {
            buf[0] = RawDictCodec.MARK_TXT;
        }

        if (matchEnd) {
            buf[buf.length-1] = RawDictCodec.MARK_ID2;
        }

        ZicoUtil.reverse(buf);

        long range = locateL(buf);

        if (range == -1L) return 0;

        int sptr = sp(range);
        int eptr = ep(range);

        boolean bskip = !matchStart;

        int cnt = 0;

        while (sptr >= 0 && sptr <= eptr) {
            int pos = bskip ? skipUntil(sptr++, RawDictCodec.MARK_ID1, true) : sptr++;
            if (pos >= 0) {
                if (bskip) pos = skip(pos, 1);
                int id = extractId(pos);
                if (!rslt.get(id)) {
                    cnt++;
                    rslt.set(id);
                }
            }

        }

        return cnt;
    }


    @Override
    public String toString() {
        return "FMI(" + isOpen() + ", " + fif.getFile() + ")";
    }

}
