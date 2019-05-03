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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.zorka.tdb.util.BitUtils.getInt48;

/**
 * Various utilities for handling and checking FM indexes.
 */
public class FmIndexTool {

    private static List<Object> lst(Object...objs) {
        return Arrays.asList(objs);
    }

    /**
     * Prints info data
     * @param data input data
     * @param out out stream
     * @param printDescriptions will print descriptions if true
     */
    public static void print(List<List<Object>> data, PrintStream out, boolean printDescriptions) {
        for (List<Object> rec : data) {
            out.println(rec.get(0) + "\t" + rec.get(1) + "\t\t\t\t" + rec.get(2));
        }
    }

    public static List<List<Object>> info(File f) {
        return info(f.getPath());
    }

    public static List<List<Object>> info(String path)  {

        try (RandomAccessFile f = new RandomAccessFile(path, "r")) {

            byte[] bha = new byte[FmIndexFormat.FM_HEADER_SZ];
            ByteBuffer bhb = ByteBuffer.wrap(bha);
            f.read(bha);

            int nblocks = bhb.getInt(FmIndexFormat.FH_NBLOCKS);
            int datalen = bhb.getInt(FmIndexFormat.FH_DATALEN);
            int nwords  = bhb.getInt(FmIndexFormat.FH_NWORDS);
            long idbase = bhb.getLong(FmIndexFormat.FH_IDBASE);

            return Arrays.asList(
                    lst("nblocks", nblocks, "Number of blocks."),
                    lst("datalen", datalen, "Raw (uncompressed) data length."),
                    lst("records", nwords, "Number of dictionary records."),
                    lst("idbase", idbase, "Base (lowest) ID.")
            );
        } catch (IOException e) {
            throw new ZicoException("I/O error", e);
        }
    }

    public static List<List<Object>> bstats(File f) {
        return bstats(f.getPath());
    }

    public static List<List<Object>> bstats(String path) {
        try (RandomAccessFile f = new RandomAccessFile(path, "r")) {

            byte[] bha = new byte[FmIndexFormat.FM_HEADER_SZ];
            ByteBuffer bhb = ByteBuffer.wrap(bha);
            f.read(bha);

            int nblocks = bhb.getInt(FmIndexFormat.FH_NBLOCKS);
            int datalen = bhb.getInt(FmIndexFormat.FH_DATALEN);
            int nwords  = bhb.getInt(FmIndexFormat.FH_NWORDS);
            long idbase = bhb.getLong(FmIndexFormat.FH_IDBASE);
            long filelen = f.length();

            byte[] bda = new byte[nblocks * FmIndexFormat.FM_BLKDSC_SZ];
            ByteBuffer bdb = ByteBuffer.wrap(bda);

            f.seek(FmIndexFormat.FM_HEADER_SZ + FmIndexFormat.FM_RANKS_SZ);
            if (nblocks * FmIndexFormat.FM_BLKDSC_SZ != f.read(bda)) {
                throw new ZicoException("Cannot read block descriptors.");
            }


            long nbcnt=0, cbcnt=0, ubcnt=0, nblen=0, cblen=0, ublen=0, rmcnt = 0, rmlen = 0;
            int[] rmh = new int[256];

            for (int i = 0; i < nblocks; i++) {
                int flags = bdb.get(i * FmIndexFormat.FM_BLKDSC_SZ + FmIndexFormat.FB_FLAGS) & 0xff;
                int tlen = bdb.get(i * FmIndexFormat.FM_BLKDSC_SZ + FmIndexFormat.FB_RANK) & 0xff;
                long loffs0 = bdb.getInt(i * FmIndexFormat.FM_BLKDSC_SZ + FmIndexFormat.FB_LOFFS);
                long loffs1 = i == nblocks - 1 ? datalen : bdb.getInt((i+1) * FmIndexFormat.FM_BLKDSC_SZ + FmIndexFormat.FB_LOFFS);
                long llen = loffs1 - loffs0;
                if (0 != (flags & FmIndexFormat.BF_DATA)) {
                    rmlen += 5*tlen;
                    rmh[tlen]++;
                    rmcnt++;
                    if (0 != (flags & FmIndexFormat.BF_COMPRESS)) {
                        // Compressed block
                        cbcnt++; cblen += llen;
                    } else {
                        // Uncompressed block
                        ubcnt++; ublen += llen;
                    }
                } else {
                    // Non-data block
                    nbcnt++; nblen += llen;
                }
            }

            long rdlen = filelen - FmIndexFormat.FM_HEADER_SZ - FmIndexFormat.FM_RANKS_SZ - (nblocks * FmIndexFormat.FM_BLKDSC_SZ) - rmlen;

            List<Integer> rmhist = new ArrayList<>(256);
            long rmhsum = 0;
            for (int i = 0; i < rmh.length; i++) {
                int n = rmh[i];
                rmhsum += i * n;
                rmhist.add(n);
            }

            return Arrays.asList(
                    lst("nblocks", nblocks, "Number of blocks."),
                    lst("filelen", filelen, "FM file size."),
                    lst("datalen", datalen, "Raw (uncompressed) data length."),
                    lst("datapct", 100.0 * filelen / datalen, "Overall compression."),
                    lst("nbcnt", nbcnt, "Non-data blocks count."),
                    lst("nblen", nblen, "Non-data blocks summary length."),
                    lst("nbpct", 100.0 * nblen / datalen, "Non-data blocks percentage."),
                    lst("cbcnt", cbcnt, "Compressed blocks count."),
                    lst("cblen", cblen, "Compressed blocks summary length."),
                    lst("cbpct", 100.0 * cblen / datalen, "Compressed blocks percentage."),
                    lst("ubcnt", ubcnt, "Uncompressed blocks count."),
                    lst("ublen", ublen ,"Uncompressed blocks summary length."),
                    lst("ubpct", 100.0 * ublen / datalen, "Uncompressed blocks percentage."),
                    lst("rmcnt", rmcnt, "Rank maps count."),
                    lst("rmlen", rmlen, "Rank maps summary length."),
                    lst("rmpct", 100.0 * rmlen / filelen, "Rank maps occupation (as percentage of whole index file"),
                    lst("rdlen", rdlen, "Raw data length."),
                    lst("rdpct", 100.0 * rdlen / filelen, "Raw data occupation (as percentage of whole index file"),
                    lst("rmhsum", rmhsum, "Number of all rank records across this file."),
                    lst("rmhist", rmhist, "Rank map length histogram")
                    // TODO block length histogram
                    // TODO compression ratio histogram
            );
        } catch (IOException e) {
            throw new ZicoException("I/O error", e);
        }
    }

}
