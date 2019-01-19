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

/**
 * Basic constants and functions regarding FM index format.
 */
public class FmIndexFormat {

    /** Main header size. */
    public final static int FM_HEADER_SZ = 32;

    /** Master rank table size. */
    public final static int FM_RANKS_SZ = 1024;

    /** Block descriptor size. */
    public final static int FM_BLKDSC_SZ = 14;

    /** Rank map entry size. */
    public final static int FM_RMAP_SZ = 7;

    // File header fields

    /** Magic header (FM signature). */
    public final static int FH_MAGIC = 0x5A464D30;

    /** Number of blocks. */
    public final static int FH_NBLOCKS = 4;

    /** BWT string data length. */
    public final static int FH_DATALEN = 8;

    /** Number of dictionary entries in this index. */
    public final static int FH_NWORDS = 12;

    /** ID base for this index. */
    public final static int FH_IDBASE = 16;

    /** Master checksum */
    public final static int FH_CKSUM = 24;


    public static final int FH_PIDX = 28;

    // Block descriptor fields

    /** Physical offset (in index file) */
    public final static int FB_POFFS = 0;

    /** Logical offset */
    public final static int FB_LOFFS = 4;

    /** Check sum */
    public final static int FB_CKSUM = 8;

    /** Character rank (for no-data blocks) */
    public final static int FB_RANK  = 12;

    /** Block flags */
    public final static int FB_FLAGS = 13;


    // Block flags

    /** This block contains data (compressed or uncompressed). */
    public final static int BF_DATA = 0x01;

    /** Block data is compressed. */
    public final static int BF_COMPRESS = 0x02;

    /** Indicates that this block contains full rank map. */
    public final static int BF_FULL_RMAP = 0x04;

}
