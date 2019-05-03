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
import io.zorka.tdb.util.lz4.LZ4Compressor;
import io.zorka.tdb.util.lz4.LZ4HCJavaSafeCompressor;
import io.zorka.tdb.util.BitUtils;

import java.io.*;
import java.util.*;
import java.util.zip.CRC32;

import static io.zorka.tdb.util.BitUtils.BYTE_ARRAY_OFFS;
import static io.zorka.tdb.util.BitUtils.UNSAFE;


/**
 * This indexer creates FM index with several types of data blocks as described in FM_INDEX_FORMAT.md.
 */
public class FmIndexFileStoreBuilder implements FmIndexStoreBuilder, Closeable {

    /** Maximum size for data blocks (excl. rank data). */
    private int blksz = 1024;

    /** Minimum gap size in order to convert it to type 2 block. Gap is a sequence of consecutive same characters. */
    private int gapsz = 270;

    /** Minimum compression ratio for storing block as compressed (as percent of original size). */
    private int crmin = 90;

    /** Minimum block size in order to be compressed. */
    private int csmin = 64;

    /** Determines how often we save full map of all character ranks (every dbgap data blocks) */
    private int dbgap = 32;

    /** Temporary buffer. */
    private byte[] buf = new byte[65536];

    /** Temporary buffer length (remainder from previous datachunk) */
    private int buflen = 0;

    /** */
    private int datalen = 0;

    /** Physical position, logical position. */
    int ppos = 0, lpos = 0;

    /** Block number */
    int bnum = 0;

    private static ThreadLocal<LZ4Compressor> lz4 = ThreadLocal.withInitial(LZ4HCJavaSafeCompressor::new);

    private File path;

    /** Output file */
    private RandomAccessFile raf;

    /** List of data blocks */
    private List<FmBlockDesc> blocks = new ArrayList<>();

    private int[] ranks = new int[256];

    private final BitSet rset = new BitSet(256);
    private final BitSet fset = new BitSet(256);
    private final int[] deltas = new int[256];
    private final CRC32 crc = new CRC32();
    private final LZ4Compressor compressor = lz4.get();
    private final byte[] ibuf = new byte[8192];
    private final byte[] rca = new byte[2048];


    public FmIndexFileStoreBuilder(File path) throws IOException {
        this(path, FmCompressionLevel.DEFAULT);
    }


    /**
     * Creates new FM index builder instance using predefined compression level.
     * @param level predefined compression level
     */
    public FmIndexFileStoreBuilder(File path, FmCompressionLevel level) throws IOException {
        this(path, level.blksz, level.gapsz, level.crmin, level.csmin, level.dbgap);
    }


    /**
     * Creates new indexer instance.
     * @param blksz maximum block size (for compressed blocks)
     * @param gapsz minimum gap size for creating type 2 block
     * @param crmin minimum compression ratio for storing compressed block
     * @param csmin minimum block size in order to be eligible for compression;
     */
    public FmIndexFileStoreBuilder(File path, int blksz, int gapsz, int crmin, int csmin, int dbgap) throws IOException {
        this.blksz = blksz;
        this.gapsz = gapsz;
        this.crmin = crmin;
        this.csmin = csmin;
        this.dbgap = dbgap;

        this.path = path;
        this.raf = new RandomAccessFile(path.getPath() + ".tmp", "rw");
        this.raf.setLength(0);
    }


    /**
     *
     * @param data
     * @return
     */
    public List<FmBlockDesc> findGaps(byte[] data, int offs, int len) {
        List<FmBlockDesc> bdesc = new ArrayList<>();

        int pos = offs, end = offs + len, gap = 0;

        while (pos < end) {
            if (data[pos] != data[gap]) {
                if (pos - gap >= gapsz) {
                    bdesc.add(new FmBlockDesc(gap, pos - gap));
                }
                gap = pos;
            }
            pos++;
        }

        // Last gap (if exists)
        if (pos - gap >= gapsz) {
            bdesc.add(new FmBlockDesc(gap, pos - gap));
        }

        return bdesc;
    }



    public List<FmBlockDesc> findBlocks(List<FmBlockDesc> gaps, int offs, int len) {
        List<FmBlockDesc> bdesc = new ArrayList<>(gaps.size() * 4);

        int pos = offs;

        for (FmBlockDesc gap : gaps) {
            while (pos < gap.getLoffs()) {
                int llen = gap.getLoffs() - pos < blksz ? gap.getLoffs() - pos : blksz;
                bdesc.add(new FmBlockDesc(pos, llen, FmIndexFormat.BF_DATA));
                pos += llen;
            }
            bdesc.add(gap);
            pos = gap.getLoffs() + gap.getLlen();
        }

        while (pos < len) {
            int llen = len - pos < blksz ? len - pos : blksz;
            bdesc.add(new FmBlockDesc(pos, llen, FmIndexFormat.BF_DATA));
            pos += llen;
        }

        return bdesc;
    }


    /**
     * This is most CPU & memory consuming part of index creation. It requires 6x more memory than WAL size.
     * @param wbuf WAL data (non-transformed raw data)
     * @return BWT transformed WAL content
     */
    public static byte[] walToBwt(byte[] wbuf) {
        int[] ibuf = new int[wbuf.length];
        byte[] bbuf = new byte[wbuf.length];

        BWT.bwtencode(wbuf, bbuf, ibuf, wbuf.length);

        return bbuf;
    }


    /**
     * Appends block/chunk of data to FM file.
     * @param bdesc block descriptor (for fast insertion of non-data blocks);
     * @param data raw data chunk
     * @throws IOException
     */
    @Override
    public void write(FmBlockDesc bdesc, byte[] data, int offs, int len) throws IOException {

        if (data == null && bdesc == null) {
            throw new ZicoException("Invalid arguments, either bdesc or data must be not null.");
        }

        List<FmBlockDesc> nblks = data == null
                ? Collections.singletonList(bdesc)
                : findBlocks(findGaps(data, offs, len), offs, len);

        if (blocks.size() > 0) {
            FmBlockDesc oblk = blocks.get(0);
            FmBlockDesc nblk = nblks.get(0);

            if (!oblk.hasData() && !nblk.hasData() && oblk.getChar() == nblk.getChar()) {
                int l = nblk.getLlen();
                oblk.setLlen(l);
                offs += l;
                len -= l;
                lpos += l;
                nblks = nblks.subList(1, nblks.size());
            }
        }

        // TODO check beginning of data[] if it matches last non-data block

        if (data == null || nblks.size() == 0) {
            return;
        }

        int lp = offs;   // Local position (in data buffer)


        // Rank map buffer

        for (FmBlockDesc blk : nblks) {
            blk.setPoffs(ppos);

            if (blk.hasData()) {

                writeRmap(data, lp, blk);
                writeData(data, lp, blk);

                for (int i = 0; i < ranks.length; i++) {
                    ranks[i] += deltas[i];
                    deltas[i] = 0;
                }

                blk.setCksum((int) crc.getValue());

            } else {
                // Non-data block
                int c = data[lp] & 0xff;
                blk.setCksum(ranks[c]);
                blk.setRsize(c);
                ranks[c] += blk.getLlen();
            }
            lp += blk.getLlen();
            bnum++;
            blk.setLoffs(lpos + blk.getLoffs());
            blocks.add(blk);
        } // for ( .. )

        lpos    += len;
        datalen += len;
    }

    private void writeData(byte[] data, int lp, FmBlockDesc blk) throws IOException {
        // Save block data
        crc.reset();
        long llen = blk.getLlen();
        System.arraycopy(data, lp, ibuf, 0, (int)llen);
        if (llen < csmin) {
            // Block too small. Save uncompressed.
            raf.write(ibuf, 0, (int)llen); // TODO llen is always small for data blocks, should be int
            crc.update(data, lp, (int)llen);
            ppos += llen;
        } else {
            int clen = compressor.compress(ibuf, 0, (int)llen, buf, 0, 131072);
            if (clen < llen * crmin / 100) {
                // Compression went well enough. Save compressed block.
                raf.write(buf, 0, clen);
                crc.update(buf, 0, clen);
                blk.setCompressed(true);
                ppos += clen;
            } else {
                // Block not compressible enough. Save uncompressed.
                raf.write(ibuf, 0, (int)llen);
                crc.update(data, lp, (int)llen);
                ppos += llen;
            }
        }
    }

    private void writeRmap(byte[] data, int lp, FmBlockDesc blk) throws IOException {

        long rcp = BYTE_ARRAY_OFFS;

        // Character ranks
        rset.clear();
        for (int i = 0; i < blk.getLlen(); i++) {
            int ci = data[i+lp] & 0xff;
            rset.set(ci);
            deltas[ci]++;
        }

        // Every <dbgap> data blocks we save ranks of all characters
        fset.clear();
        if (bnum % dbgap == 0) {
            for (int i = 0; i < 256; i++) {
                if (ranks[i] != 0) {
                    fset.set(i);
                }
            }
            blk.setFullMap(true);
        }

        int rlen = 0;

        for (int i = 0; i < 256; i++) {
            if (rset.get(i) || fset.get(i)) {
                rlen++;
            }
        }

        // Save rank map
        int ri = 0;
        for (int i = 0; i < 256; i++) {
            if (rset.get(i)) {
                UNSAFE.putByte(rca, rcp+ri, (byte)i);
                UNSAFE.putInt(rca, rcp+rlen+ri*4, ranks[i]);
                UNSAFE.putShort(rca, rcp+rlen*5+ri*2, (short) deltas[i]);
                ri++;
            } else if (fset.get(i)) {
                // Negative rank values mean that character does not exist in datablock, so block does not
                // need to be checked when determining rank of given character.
                UNSAFE.putByte(rca, rcp+ri, (byte)i);
                UNSAFE.putInt(rca, rcp+rlen+ri*4, -ranks[i]);
                UNSAFE.putShort(rca, rcp+rlen*5+ri*2, (short)0);
                ri++;
            }
        }

        raf.write(rca, 0, rlen * FmIndexFormat.FM_RMAP_SZ);
        ppos += rlen * FmIndexFormat.FM_RMAP_SZ;
        blk.setRsize(rlen);

        crc.reset();
        crc.update(rca, 0, rlen * FmIndexFormat.FM_RMAP_SZ);
    }


    @Override
    public void finish(int nwords, long idbase, int pidx) throws IOException {
        int bds = FmIndexFormat.FM_HEADER_SZ + FmIndexFormat.FM_RANKS_SZ + FmIndexFormat.FM_BLKDSC_SZ * blocks.size();
        byte[] bda = new byte[bds];
        long bdp = BitUtils.BYTE_ARRAY_OFFS + FmIndexFormat.FM_HEADER_SZ;


        // Prepare header, ranks, block descriptors
        // Prepare master rank table
        for (int rank : ranks) {
            UNSAFE.putInt(bda, bdp, rank); bdp += 4;
        }

        // Prepare block descriptors table
        for (FmBlockDesc blk : blocks) {
            UNSAFE.putInt(bda, bdp, blk.getPoffs() + bds); bdp += 4;
            UNSAFE.putInt(bda, bdp, blk.getLoffs()); bdp += 4;
            UNSAFE.putInt(bda, bdp, blk.getCksum()); bdp += 4;
            UNSAFE.putByte(bda, bdp++, (byte)blk.getRsize());
            UNSAFE.putByte(bda, bdp++, (byte)blk.getFlags());
        }


        bdp = BitUtils.BYTE_ARRAY_OFFS;

        UNSAFE.putInt(bda, bdp ,FmIndexFormat.FH_MAGIC);
        UNSAFE.putInt(bda, bdp+ FmIndexFormat.FH_NBLOCKS, blocks.size());
        UNSAFE.putInt(bda, bdp+ FmIndexFormat.FH_DATALEN, datalen);
        UNSAFE.putInt(bda, bdp+ FmIndexFormat.FH_NWORDS, nwords);
        UNSAFE.putLong(bda ,bdp+ FmIndexFormat.FH_IDBASE, idbase);
        UNSAFE.putInt(bda, bdp+ FmIndexFormat.FH_PIDX, pidx);
        UNSAFE.putInt(bda, bdp+ FmIndexFormat.FH_CKSUM, 0);

        CRC32 crc = new CRC32();
        crc.update(bda);
        UNSAFE.putInt(bda, bdp+ FmIndexFormat.FH_CKSUM, (int)crc.getValue());

        try (RandomAccessFile f = new RandomAccessFile(path, "rw")) {
            f.setLength(0);
            f.write(bda);

            raf.seek(0);

            byte[] buf = new byte[1024 * 1024];
            int pos = 0;

            while (pos < ppos) {
                int l = raf.read(buf);
                f.write(buf, 0, l);
                pos += l;
            }
        }

        close();

        if (!new File(path.getPath() + ".tmp").delete()) {
            throw new IOException("Cannot remove temporary file.");
        }

    }
    
    @Override
    public void close() throws IOException {
        if (raf != null) {
            raf.close();
            raf = null;
        }
    }
}
