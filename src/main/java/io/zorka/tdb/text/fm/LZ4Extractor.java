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

import io.zorka.tdb.ZicoException;
import io.zorka.tdb.ZicoException;
import io.zorka.tdb.util.BitUtils;

import static io.zorka.tdb.util.BitUtils.UNSAFE;

/**
 * Simplified LZ4 decompressor. Note that this is NOT general purpose decompressor class.
 * This implementation is intended for fast extraction of single characters and their ranks
 * from randomly accessed compressed blocks. Not for general purpose.
 *
 * See http://fastcompression.blogspot.in/2011/05/lz4-explained.html for full format description.
 */
public class LZ4Extractor {

    public final static int BUF_SIZE = 65536;

    private final static int LSHIFT = 4;
    private final static int LMASK  = 0x0f;

    private final static int MINMATCH = 4;

    private byte[] buf = new byte[BUF_SIZE];

    private final static byte[] ZERO = new byte[BUF_SIZE];

    public static byte chr(int car) {
        return (byte)(car & 0xff);
    }

    public static int rank(int car) {
        return car >>> 8;
    }


    /**
     * Decompresses chunk of data from direct buffer limited by len up to position pos.
     * Maximum uncompressed data block size is 64kB. This method uses Unsafe in order
     * to maximize performance.
     *
     * @param addr - physical address of compressed data
     * @param len - buffer length (limit for compressor)
     * @param pos - character position (in uncompressed stream)
     * @param chr - character which rank will be returned (-1 if character at position pos to be used);
     * @return character at pos (lower 8 bits), rank at next 16 bits;
     */
    public int charAndRank(long addr, int len, int pos, int chr) {
        long iptr = addr, end = iptr + len;
        int optr = 0;

        while (iptr < end) {
            int token = BitUtils.UNSAFE.getByte(iptr++) & 0xff;

            // Decode literal length (base)
            int llen = (token >>> LSHIFT) & LMASK;

            if (llen == LMASK) {
                int l;
                do {
                    // TODO check bounds here
                    l = BitUtils.UNSAFE.getByte(iptr++) & 0xff;
                    llen += l;
                } while (l == 255);
            }

            // Copy literal from input buffer to output

            for (int i = 0; i < llen; i++) {
                // TODO check bounds here
                buf[optr++] = BitUtils.UNSAFE.getByte(iptr++);
            }

            if (optr > pos) {
                return charAndRank(pos, chr);
            }

            // Decode match offset
            // TODO check bounds here
            int moffs = (BitUtils.UNSAFE.getByte(iptr++) & 0xff) | ((BitUtils.UNSAFE.getByte(iptr++) & 0xff) << 8);

            if (moffs != 0) {
                if (moffs <= 0 || moffs >= 65536) throw new ZicoException("Invalid match offset: " + moffs);

                // Decode match length
                int mlen = token & LMASK;

                if (mlen == LMASK) {
                    int l;
                    do {
                        // TODO check bounds here
                        l = BitUtils.UNSAFE.getByte(iptr++) & 0xff;
                        mlen += l;
                    } while (l == 255);
                }

                mlen += MINMATCH;

                // Copy match

                int mpos = optr - moffs;

                if (optr + mlen > pos + 1) {
                    mlen = pos + 1 - optr;
                }

                if (mlen > 0) {
                    for (int i = 0; i < mlen; i++) {
                        buf[optr + i] = buf[mpos + i];
                    }
                    optr += mlen;
                }

                if (optr > pos) {
                    return charAndRank(pos, chr);
                }

            }
        }

        // Should never reach this.
        throw new ZicoException("Out of compressed data: len="
                + len + ", pos=" + pos + ", iptr=" + (iptr-addr) + ", optr=" + optr);
    }

    // TODO prepare charAndRank(long addr, )

    private int charAndRank(int pos, int chr) {
        int rank = 0;

        byte ch = chr == -1 ? buf[pos] : (byte)chr;

        for (int i = 0; i < pos; i++) {
            if (buf[i] == ch) {
                rank++;
            }
        }

        return (ch & 0xff) | (rank << 8);
    }


    /**
     * Clears extractor buffer.
     * Can be used to ensure extractor initial state be always the samebut its cost is non-trivial.
     */
    public void reset() {
        System.arraycopy(ZERO, 0, buf, 0, ZERO.length);
    }


    public int charAndRank(long addr, int len, int pos) {
        return charAndRank(addr, len, pos, -1);
    }
}
