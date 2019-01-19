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

package io.zorka.tdb.store;

import io.zorka.tdb.ZicoException;
import io.zorka.tdb.util.CborBufReader;
import io.zorka.tdb.util.lz4.LZ4Compressor;
import io.zorka.tdb.util.lz4.LZ4Decompressor;
import io.zorka.tdb.util.lz4.LZ4HCJavaSafeCompressor;
import io.zorka.tdb.util.lz4.LZ4JavaSafeDecompressor;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 *
 */
public class RawTraceDataFile implements Closeable {

    public static final int HDR_MAGIC = 0x5a545330;
    public static final int CHK_MAGIC = 0x5a434830;


    public static final int NO_COMPRESSION   = 0x00000000;
    public static final int ZLIB_COMPRESSION = 0x00000001;
    public static final int LZ4_COMPRESSION  = 0x00000002;
    public static final int COMPRESSION_MASK = 0x0000000F;
    public static final int CHECKSUM_MASK    = 0x000000F0;
    public static final int NO_CHECKSUM      = 0x00000000;
    public static final int CRC32_CHECKSUM   = 0x00000010;

    private final RandomAccessFile raf;

    private File file;

    private int flags = NO_COMPRESSION;

    private byte[] buffer;


    public RawTraceDataFile(File file, boolean rw) {
        this(file, rw, ZLIB_COMPRESSION | CRC32_CHECKSUM);
    }

    public RawTraceDataFile(File file, boolean rw, int flags) {
        this.file = file;
        this.flags = flags;

        try {
            raf = new RandomAccessFile(file, rw ? "rw" : "r");
            readHeaders(flags);
        } catch (IOException e) {
            throw new ZicoException("Cannot open trace data file " + file, e);
        }
    }


    private void readHeaders(int flags) throws IOException {
        if (raf.length() == 0) {
            raf.writeInt(HDR_MAGIC);
            raf.writeInt(flags);
            raf.writeInt(0);   // checksum
            raf.writeInt(0);   // reserved
        } else {
            int magic = raf.readInt();
            if (magic != HDR_MAGIC) {
                throw new ZicoException("Invalid file header.");
            }
            this.flags = raf.readInt();
        }
    }


    public CborBufReader read(long pos) {
        try {
            int csize, lsize;
            long cksum;
            byte[] cbuf, lbuf;
            synchronized (raf) {
                raf.seek(pos);  // TODO synchronize only file access, not whole method
                if (raf.readInt() != CHK_MAGIC) {
                    throw new IOException("Invalid trace header at offset " + pos + " of " + file);
                }

                csize = raf.readInt();
                lsize = raf.readInt();
                cksum = raf.readInt() & 0xFFFFFFFFL;
                cbuf = new byte[csize];    // TODO make cbuf GC-free

                raf.read(cbuf);
            }

            if (0 != (flags & CRC32_CHECKSUM)) {
                CRC32 crc = new CRC32();
                crc.update(cbuf);

                if (crc.getValue() != cksum) {
                    throw new ZicoException("Checksum error for chunk at position " + pos + " of " + file);
                }
            }


            switch (flags & COMPRESSION_MASK) {
                case NO_COMPRESSION: {
                    lbuf = cbuf;
                    break;
                }
                case ZLIB_COMPRESSION: {
                    lbuf = new byte[lsize];
                    Inflater inflater = new Inflater(true);
                    inflater.setInput(cbuf, 0, cbuf.length);
                    try {
                        if (inflater.inflate(lbuf) != lbuf.length) {
                            throw new IOException("Invalid length of uncompressed data.");
                        }
                    } catch (DataFormatException e) {
                        throw new IOException("Malformed compressed chunk data at position " + pos + " of " + file, e);
                    }
                    break;
                }
                case LZ4_COMPRESSION: {
                    lbuf = new byte[lsize];
                    LZ4Decompressor dec = LZ4JavaSafeDecompressor.INSTANCE;
                    if (cbuf.length != dec.decompress(cbuf, 0, lbuf, 0, lbuf.length)) {
                        throw new ZicoException("Malformed compressed chunk data at position " + pos + " of " + file);
                    }
                    break;
                }
                default:
                    throw new IOException("Invalid compression algorithm: " + (flags & COMPRESSION_MASK));
            }

            return new CborBufReader(lbuf);

        } catch (IOException e) {
            throw new ZicoException("Error reading trace chunk: " + e.getMessage(), e);
        }
    }


    public long write(byte[] lbuf) {
        return write(lbuf, 0, lbuf.length);
    }


    public long write(byte[] lbuf, int loffs, int llen)  {

        byte[] cbuf = new byte[llen + 1024];
        int clen = 0, chksum = 0;

        switch (flags & COMPRESSION_MASK) {
            case NO_COMPRESSION: {
                System.arraycopy(lbuf, loffs, cbuf, 0, llen);
                clen = llen;
                break;
            }
            case ZLIB_COMPRESSION: {
                Deflater deflater = new Deflater(6, true);
                deflater.setInput(lbuf, loffs, llen);
                clen = deflater.deflate(cbuf, 0, cbuf.length, Deflater.FULL_FLUSH);
                deflater.finish();
                break;
            }
            case LZ4_COMPRESSION: {
                LZ4Compressor compressor = LZ4HCJavaSafeCompressor.INSTANCE;
                clen = compressor.compress(lbuf, loffs, llen, cbuf, 0, cbuf.length);
                break;
            }
            default:
                throw new ZicoException("Invalid compression algorithm: " + (flags & COMPRESSION_MASK));
        }

        if (0 != (flags & CRC32_CHECKSUM)) {
            CRC32 crc = new CRC32();
            crc.update(cbuf, 0, clen);
            chksum = (int)crc.getValue();
        }

        try {
            synchronized (raf) {
                long pos = raf.length();
                this.raf.seek(pos);
                raf.writeInt(CHK_MAGIC);
                raf.writeInt(clen);
                raf.writeInt(llen);
                raf.writeInt(chksum);
                raf.write(cbuf, 0, clen);
                return pos;
            }
        } catch (IOException e) {
            throw new ZicoException("I/O exception when writing block ");
        }
    }


    public long length() {
        try {
            synchronized (raf) {
                return raf.length();
            }
        } catch (IOException e) {
            throw new ZicoException("I/O error", e);
        }
    }

    @Override
    public void close() throws IOException {
        raf.close();
    }
}


