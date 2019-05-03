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
import io.zorka.tdb.util.ZicoUtil;
import io.zorka.tdb.util.lz4.LZ4Decompressor;
import io.zorka.tdb.util.lz4.LZ4JavaSafeDecompressor;
import io.zorka.tdb.util.BitUtils;
import sun.nio.ch.DirectBuffer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.zorka.tdb.text.fm.FmIndexStore.car;

/**
 * Memory mapped FM index.
 */
public class FmIndexFileStore implements Closeable, FmIndexStore {

    // FmIndexFile open flags.

    /** Verify main checksum after open. */
    public final static int CHECK_MAIN_SUM = 0x01;

    /** Verify block checksums (lazy - only when block is accessed for the first time). */
    public final static int CHECK_BLK_SUMS = 0x02;

    /** Verify all checksums. */
    public final static int CHECK_ALL_SUMS = CHECK_MAIN_SUM | CHECK_BLK_SUMS;

    private long fmaddr, fmlimit;
    private ByteBuffer fmbuf;

    private int[] charOffsets = new int[256];

    private int idBase;

    /** */
    private int flags;

    /** BWT data length, physical file length, number of blocks, logical offset for last block */
    private int datalen, filelen, nblocks, lastLogicalOffs, nwords, pidx;

    private String path;
    private RandomAccessFile raf;
    private FileChannel channel;
    private File file;
    private long fmdata;


    private static ThreadLocal<LZ4Extractor> lz4 = ThreadLocal.withInitial(LZ4Extractor::new);

    public FmIndexFileStore(File f, int flags) {
        this(f.getPath(), flags);
    }

    public FmIndexFileStore(String path, int flags) {
        this.path = path;
        this.flags = flags;
        try {
            open();
            init();
        } catch (IOException e) {
            throw new ZicoException("I/O error", e);
        }
    }


    /**
     * Opens and maps index file. After invocation, fmbuf contains mapped contents of index file.
     * @throws IOException
     */
    private void open() throws IOException {
        file = new File(path);
        if (!file.isFile() || !file.canRead()) {
            throw new ZicoException("File " + path + " does not exist or is not readable.");
        }
        raf = new RandomAccessFile(file, "r");
        channel = raf.getChannel();
        fmbuf = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        fmaddr = ((DirectBuffer)fmbuf).address();      // TODO test this on various JVMs
        fmlimit = fmaddr + raf.length();
        fmdata = fmaddr + FmIndexFormat.FM_HEADER_SZ + FmIndexFormat.FM_RANKS_SZ;
        filelen = (int)file.length();
    }


    @Override
    public void close() throws IOException {
        if (raf != null) {
            channel.close(); channel = null;
            raf.close(); raf = null;
            ZicoUtil.unmapBuffer(fmbuf); fmbuf = null;
        }
        fmaddr = 0;
    }


    private void checkOpen() {
        if (fmaddr == 0) {
            throw new ZicoException("FM index file store is closed.");
        }
    }


    /**
     * Reads file header, performs basic checks.
     */
    private void init() {
        if (BitUtils.UNSAFE.getInt(fmaddr) != FmIndexFormat.FH_MAGIC) {
            throw new ZicoException("File " + file + " is not FM index.");
        }
        nblocks = BitUtils.UNSAFE.getInt(fmaddr+ FmIndexFormat.FH_NBLOCKS);
        datalen = BitUtils.UNSAFE.getInt(fmaddr+ FmIndexFormat.FH_DATALEN);
        nwords  = BitUtils.UNSAFE.getInt(fmaddr+ FmIndexFormat.FH_NWORDS);
        idBase  = BitUtils.UNSAFE.getInt(fmaddr+ FmIndexFormat.FH_IDBASE);
        pidx    = BitUtils.UNSAFE.getInt(fmaddr+ FmIndexFormat.FH_PIDX);

        // Check if file is long enough to contain all block descriptors
        if (filelen < FmIndexFormat.FM_HEADER_SZ + FmIndexFormat.FM_RANKS_SZ + FmIndexFormat.FM_BLKDSC_SZ * nblocks) {
            throw new ZicoException("Unexpected end of file: " + file);
        }

        // TODO Verify master checksum
//        if (0 != (flags & CHECK_MAIN_SUM)) {
//            if (ZicoUtil.crc32(fmaddr + FM_HEADER_SZ, nblocks * FM_BLKDSC_SZ + FM_RANKS_SZ, 0) != fmbuf.readInt(24)) {
//                throw new ZicoException("Master checksum error when opening file " + file);
//            }
//        }

        lastLogicalOffs = getBlkLOffs(nblocks-1);

        // Read master rank table and calculate offsets
        for (int i = 0; i < charOffsets.length; i++) {
            charOffsets[i] = BitUtils.UNSAFE.getInt(fmaddr+ FmIndexFormat.FM_HEADER_SZ + i*4);
        }

        for (int i = 1; i < charOffsets.length; i++) {
            charOffsets[i] += charOffsets[i-1];
        }

        for (int i = charOffsets.length-1; i > 0; i--) {
            charOffsets[i] = charOffsets[i-1];
        }

        charOffsets[0] = 0;
    }

    private int getBlkPOffs(int blk) {
        return BitUtils.UNSAFE.getInt(fmdata + blk * FmIndexFormat.FM_BLKDSC_SZ + FmIndexFormat.FB_POFFS);
    }

    private int getBlkLOffs(int blk) {
        return BitUtils.UNSAFE.getInt(fmdata + blk * FmIndexFormat.FM_BLKDSC_SZ + FmIndexFormat.FB_LOFFS);
    }

    private int getBlkCksum(int blk) {
        return BitUtils.UNSAFE.getInt(fmdata + blk * FmIndexFormat.FM_BLKDSC_SZ + FmIndexFormat.FB_CKSUM);
    }

    private int getBlkFlags(int blk) {
        return BitUtils.UNSAFE.getByte(fmdata + blk * FmIndexFormat.FM_BLKDSC_SZ + FmIndexFormat.FB_FLAGS) & 0xff;
    }

    private int getBlkRSize(int blk) {
        return BitUtils.UNSAFE.getByte(fmdata + blk * FmIndexFormat.FM_BLKDSC_SZ + FmIndexFormat.FB_RANK) & 0xff;
    }

    private int getBlkPLen(int blk) {
        int offs1 = getBlkPOffs(blk);
        int offs2 = blk == nblocks - 1 ? filelen : getBlkPOffs(blk + 1);
        return offs2 - offs1;
    }

    private int getBlkLLen(int blk) {
        return (blk == nblocks-1 ? datalen : getBlkLOffs(blk+1)) - getBlkLOffs(blk);
    }


    /** Locates block containing position loffs. Returns block index. */
    private int findBlock(int loffs) {
        if (loffs < 0 || loffs >= datalen) {
            throw new ZicoException("Invalid logical offset: " + loffs);
        }

        if (loffs >= lastLogicalOffs) {
            return nblocks-1;
        }

        int blk1 = 0, blk2 = nblocks - 1;

        while (blk1 < blk2) {
            int blk = (blk1 + blk2) >>> 1;
            int lf1 = getBlkLOffs(blk), lf2 = getBlkLOffs(blk+1);

            if (loffs >= lf1 && loffs < lf2) {
                return blk;
            } else if (loffs < lf1) {
                blk2 = blk;
            } else {
                blk1 = blk;
            }
        }

        if (loffs >= getBlkLOffs(blk1) && loffs < getBlkLOffs(blk1+1)) {
            return blk1;
        }

        throw new ZicoException("Cannot find block containig position " + loffs + ". Check if FM file is not corrupted.");
    }


    @Override
    public long charAndRank(int pos) {

        checkOpen();

        int blk = findBlock(pos), rank = 0;
        int loffs = getBlkLOffs(blk), poffs = getBlkPOffs(blk), cksum = getBlkCksum(blk);
        int flags = getBlkFlags(blk), rsize = getBlkRSize(blk), c;

        if (0 != (flags & FmIndexFormat.BF_DATA)) {
            // Data block

            long boffs = poffs + rsize * FmIndexFormat.FM_RMAP_SZ;

            // Obtain character and fix up its rank.
            if (0 != (flags & FmIndexFormat.BF_COMPRESS)) {
                // Compressed block
                int lpos = pos - loffs;
                int plim = getBlkPLen(blk) - rsize * FmIndexFormat.FM_RMAP_SZ;
                int car = lz4.get().charAndRank(fmaddr + boffs, plim, lpos);
                c = LZ4Extractor.chr(car) & 0xff;
                rank = LZ4Extractor.rank(car);
            } else {
                // Uncompressed block
                c = BitUtils.UNSAFE.getByte(fmaddr + boffs + (pos - loffs)) & 0xff;
                for (int i = 0; i < pos - loffs; i++) {
                    if ((BitUtils.UNSAFE.getByte(fmaddr + boffs + i) & 0xff) == c) {
                        rank++;
                    }
                }
            }

            // Search in rank map
            for (int i = 0; i < rsize; i++) {
                // This thing is short enough for linear search
                if ((BitUtils.UNSAFE.getByte(fmaddr + poffs + i) & 0xff) == c) {
                    rank += BitUtils.UNSAFE.getInt(fmaddr + poffs + rsize + 4 * i);
                    break;
                }
            }
        } else {
            // Non-data block
            rank = cksum + (pos - loffs);
            c = rsize;
        }

        return car((byte)c, rank);
    }


    @Override
    public int rankOf(int pos, byte chr) {

        checkOpen();

        int c = chr & 0xff;

        if (pos <= 0) {
            return 0;
        }

        if (pos >= datalen) {
            return BitUtils.UNSAFE.getInt(fmaddr+ FmIndexFormat.FM_HEADER_SZ + c * 4);
        }

        int blk = findBlock(pos);

        for (int bn = blk; bn >= 0; bn--) {
            int loffs = getBlkLOffs(bn), cksum = getBlkCksum(bn), flags = getBlkFlags(bn), rsize = getBlkRSize(bn);

            if (0 != (flags & FmIndexFormat.BF_DATA)) {
                // Data block
                int poffs = getBlkPOffs(bn), rank = -1, delta = 0;

                // Search rank table for current block
                for (int i = 0; i < rsize; i++) {
                    if ((BitUtils.UNSAFE.getByte(fmaddr + poffs + i) & 0xff) == c) {
                        rank = BitUtils.UNSAFE.getInt(fmaddr + poffs + rsize + 4 * i);
                        if (rank < 0) {
                            return -rank;
                        }
                        delta = BitUtils.UNSAFE.getShort(fmaddr + poffs + rsize*5 + 2*i);
                        break;
                    }
                }

                if (rank >= 0) {
                    if (bn == blk) {
                        //int llen = getBlkLLen(bn);
                        int lpos = (pos - loffs);
                        long bdaddr = fmaddr + poffs + rsize * FmIndexFormat.FM_RMAP_SZ;
                        if (0 != (flags & FmIndexFormat.BF_COMPRESS)) {
                            // Compressed block
                            int plim = getBlkPLen(blk) - rsize * FmIndexFormat.FM_RMAP_SZ;
                            int rslt = lz4.get().charAndRank(bdaddr, plim, lpos, c);
                            rank += rslt >>> 8;
                        } else {
                            // Uncompressed block
                            for (int i = 0; i < lpos; i++) {
                                if ((BitUtils.UNSAFE.getByte(bdaddr + i) & 0xff) == c) {
                                    rank++;
                                }
                            }
                        }
                    } else {
                        rank += delta;
                    }

                    return rank;
                }
            } else {
                // No-data block
                if (rsize == c) {
                    return cksum + (bn == blk ? pos - loffs : getBlkLLen(bn));
                }
            }

            if (0 != (flags & FmIndexFormat.BF_FULL_RMAP)) {
                // Full map and no character rank found. Skip further lookup.
                return 0;
            }

        }

        return 0;
    }


    /**
     * Returns raw data (BWT string) from given position in file
     * @param buf buffer where raw data will be stored
     * @param lpos start position in BWT string represented by this file (logical position)
     * @return number of bytes returned
     */
    @Override
    public int getData(byte[] buf, int lpos) {

        checkOpen();

        int len = buf.length;
        byte[] ibuf = new byte[8192], obuf = new byte[8192];
        LZ4Decompressor dcmp = LZ4JavaSafeDecompressor.INSTANCE;

        // Limit output length if we overrun BWT string
        if (lpos + len > datalen) {
            len = datalen - lpos;
        }

        if (len <= 0) {
            return 0;
        }

        int bufpos = 0, blk = findBlock(lpos);

        while (bufpos < len && blk < nblocks) {
            int flags = getBlkFlags(blk);
            int blklen = Math.min(getBlkLLen(blk), len - bufpos);
            int rsize = getBlkRSize(blk);
            if (0 != (flags & FmIndexFormat.BF_DATA)) {
                int blkpos = lpos - getBlkLOffs(blk);
                long addr = fmaddr + getBlkPOffs(blk) + rsize * FmIndexFormat.FM_RMAP_SZ;
                // Data block
                if (0 != (flags & FmIndexFormat.BF_COMPRESS)) {
                    int blkPLen = getBlkPLen(blk) - rsize * FmIndexFormat.FM_RMAP_SZ;
                    if (addr + blkPLen > fmlimit)
                        throw new ZicoException("Attempt to read after the end of index file: blk=" + blk
                            + " blkPLen=" + blkPLen + " ");
                    BitUtils.UNSAFE.copyMemory(null, addr, ibuf, BitUtils.BYTE_ARRAY_OFFS, blkPLen);
                    dcmp.decompress(ibuf, 0, obuf, 0, blklen);
                    System.arraycopy(obuf, blkpos, buf, bufpos, blklen);
                } else {
                    if (addr + blkpos + blklen > fmlimit)
                        throw new ZicoException("Attempt to read after the end of index file: blk=" + blk);
                    BitUtils.UNSAFE.copyMemory(null, addr + blkpos, buf, BitUtils.BYTE_ARRAY_OFFS + bufpos, blklen);
                }
            } else {
                // Non-data block
                for (int i = 0; i < blklen; i++) {
                    buf[bufpos+i] = (byte)rsize;
                }
            }
            bufpos += blklen;
            lpos   += blklen;
            blk++;
        }

        return bufpos;
    }

    @Override
    public long length() {
        return file.length();
    }


    @Override
    public long getNBlocks() {
        checkOpen();
        return nblocks;
    }


    @Override
    public int getIdBase() {
        checkOpen();
        return idBase;
    }


    @Override
    public int getDatalen() {
        checkOpen();
        return datalen;

    }


    @Override
    public int getNWords() {
        checkOpen();

        return nwords;

    }


    @Override
    public int getCharOffs(byte ch) {
        checkOpen();
        return charOffsets[ch & 0xff];
    }


    public int getPidx() {
        checkOpen();
        return pidx;
    }


    public File getFile() {
        return file;
    }

}
