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

package io.zorka.tdb.text.fm;

/**
 * Basic description of FM index block.
 */
public class FmBlockDesc {

    /** Compressed block position in physical file (a.k.a. physical offset). */
    private int poffs;

    /** Block position in uncompressed form (a.k.a. logical offset). */
    private int loffs;

    /** Logical block length (used temporarily in this class). */
    private int llen;

    /** Block flags (see FM index format description). */
    private int flags;

    /** Rank map size (number of ranked chars). */
    private int rsize;

    /** Block checksum (xxh32, may be byte order dependent). */
    private int cksum;

    /** Compressed block length. */
    private int clen;

    /** Block data */
    private byte[] data;

    public FmBlockDesc(int loffs, int llen) {
        this.loffs = loffs;
        this.llen = llen;
    }


    public FmBlockDesc(int loffs, int llen, int flags) {
        this.loffs = loffs;
        this.llen = llen;
        this.flags = flags;
    }

    public int getPoffs() {
        return poffs;
    }

    public void setPoffs(int poffs) {
        this.poffs = poffs;
    }

    public int getLoffs() {
        return loffs;
    }

    public void setLoffs(int loffs) {
        this.loffs = loffs;
    }

    public int getLlen() {
        return llen;
    }

    public void setLlen(int llen) {
        this.llen = llen;
    }

    public int getFlags() {
        return flags;
    }

    public boolean hasData() {
        return 0 != (flags & FmIndexFormat.BF_DATA);
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public void setFullMap(boolean fullMap) {
        if (fullMap) {
            flags |= FmIndexFormat.BF_FULL_RMAP;
        } else {
            flags &= ~FmIndexFormat.BF_FULL_RMAP;
        }
    }

    public boolean isFullMap() {
        return 0 != (flags & FmIndexFormat.BF_FULL_RMAP);
    }

    public void setCompressed(boolean compressed) {
        if (compressed) {
            flags |= FmIndexFormat.BF_COMPRESS;
        } else {
            flags &= ~FmIndexFormat.BF_COMPRESS;
        }
    }

    public int getRsize() {
        return rsize;
    }

    public byte getChar() { return (byte)rsize; }

    public void setRsize(int rsize) {
        this.rsize = rsize;
    }

    public int getCksum() {
        return cksum;
    }

    public void setCksum(int cksum) {
        this.cksum = cksum;
    }

    public int getClen() {
        return clen;
    }

    public void setClen(int clen) {
        this.clen = clen;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public int hashCode() {
        return (int)(poffs * 11 + loffs * 13 + llen * 17 + flags * 19 + cksum);
    }


    @Override
    public String toString() {
        return "[" + loffs + "," + llen + "," + flags + "]";
    }


    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FmBlockDesc)) return false;
        FmBlockDesc bd = (FmBlockDesc) o;
        return poffs == bd.poffs && loffs == bd.loffs && llen == bd.llen && flags == bd.flags && cksum == bd.cksum;
    }
}
