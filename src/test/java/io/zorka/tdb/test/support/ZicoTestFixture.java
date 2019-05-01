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

package io.zorka.tdb.test.support;

import io.zorka.tdb.ZicoException;
import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.meta.StructuredTextIndex;
import io.zorka.tdb.store.*;
import io.zorka.tdb.text.*;
import io.zorka.tdb.text.fm.FmIndexFileStoreBuilder;
import io.zorka.tdb.text.fm.FmTextIndex;
import io.zorka.tdb.text.WalTextIndex;
import io.zorka.tdb.store.RecursiveTraceDataRetriever;
import io.zorka.tdb.store.RotatingTraceStore;
import org.junit.Before;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;

public class ZicoTestFixture {

    public static final int MB = 1024 * 1024;

    public static final String TESTDATA_DIR = "/v/zorka/zicodev/testdata";
    public static final String ROOT_DIR = "/tmp";

    // Sample 300MB file 
    public static final File T300M_BWT = new File(TESTDATA_DIR, "T300M.bwt");
    public static final File T300M_IFM = new File(TESTDATA_DIR, "T300M.ifm");
    public static final File T300M_RND = new File(TESTDATA_DIR, "T300M.rnd");


    public static final String tmpDir;

    protected TestStrGen randStrGen;
    protected Random rand = new Random();

    protected Map<String,TraceDataIndexer> indexerCache = new ConcurrentHashMap<>();


    protected void prepareStrings(File file, int size) {
        int flen = (int)file.length();
        if (file.exists() && flen > size) {
            return;
        }
        try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file), 1024*1024)) {
            if (randStrGen == null) {
                randStrGen = new MutatingStrGen(new JarScanStrGen(), MutatingStrGen.RANDOM, 1024);
            }
            int len = 0;
            while (len < size) {
                byte[] b = randStrGen.get().getBytes();
                os.write(b);
                os.write(0);
                len += b.length+1;
            }
        } catch (IOException e) {
            fail("Cannot create strings file: ");
        }
    }


    protected TestStrGen getRandStrGen() throws IOException {
        if (randStrGen == null) {
            randStrGen = new MutatingStrGen(
                    new VerifyingStrGen(new JarScanStrGen(), true, false),
                    MutatingStrGen.RANDOM, 32);
        }
        return randStrGen;
    }


    protected TestStrGen getSerialStrGen() throws IOException {
        if (randStrGen == null) {
            randStrGen = new MutatingStrGen(
                    new VerifyingStrGen(new JarScanStrGen(), true, false),
                    MutatingStrGen.SERIAL, 32);
        }
        return randStrGen;
    }


    @Before
    public void cleanup() {
        clearTmpDir();
    }


    static {
        tmpDir = ROOT_DIR + File.separatorChar + "zicotestdir";
        File d = new File(tmpDir);
        if (!d.isDirectory() && !d.mkdirs()) {
            fail("Cannot create test directory: " + tmpDir);
        }
    }


    private static void clearTmpDir() {
        try {
            TestUtil.rmrf(tmpDir);
        } catch (IOException e) {
            fail("Cannot cleanup test directory: " + tmpDir);
        }
        if (!new File(tmpDir).mkdirs()) {
            fail("Cannot create test directory: " + tmpDir);
        }
    }


    protected RotatingTraceStore openRotatingStore() throws Exception {
        File baseDir = new File(tmpDir, "store");
        if (!baseDir.exists()) {
            assertTrue(baseDir.mkdirs());
        }
        RotatingTraceStore store = new RotatingTraceStore(baseDir, new Properties(), s -> 1, indexerCache);
        store.open();
        return store;
    }


    protected SimpleTraceStore createSimpleStore(int id) throws Exception {
        File baseDir = new File(tmpDir, String.format("%06x", id));
        assertTrue(baseDir.mkdir());
        return new SimpleTraceStore(baseDir, null, indexerCache, s->1);
    }


    public enum IndexType {
        WAL,
        FMI
    }


    protected static TextIndex createTextIndex(IndexType type, String fname, String...terms) throws Exception {
        switch (type) {
            case WAL:
                return createWalTextIndex(fname, terms);
            case FMI:
                return createFmTextIndex(fname, terms);
        }

        throw new ZicoException("Illegal index type: " + type);
    }


    protected static WalTextIndex createWalTextIndex(String fname, String...terms) throws Exception {
        File f = fname.contains("/") ? new File(fname) : new File(tmpDir, fname + ".wal");
        if (f.exists() && !f.delete()) fail("Cannot remove old file: " + f);
        WalTextIndex wal = new WalTextIndex(f, 0, 16 * MB);
        for (String s : terms) {
            wal.add(s);
        }
        return wal;
    }


    protected static FmTextIndex createFmTextIndex(String fname, String...terms) throws Exception {
        WalTextIndex wal = createWalTextIndex(fname, terms);
        File f = fname.contains("/") ? new File(fname) : new File(tmpDir, fname + ".fmi");
        if (f.exists() && !f.delete()) fail("Cannot remove old file: " + f);
        FmIndexFileStoreBuilder fib = new FmIndexFileStoreBuilder(f);
        fib.walToFm(wal);
        wal.close();
        fib.close();
        return new FmTextIndex(f);
    }

    protected static FmTextIndex toFmIndex(WalTextIndex wal) throws Exception {
        File f = new File(wal.getPath().replace(".wal", ".fmi"));
        FmIndexFileStoreBuilder fib = new FmIndexFileStoreBuilder(f);
        fib.walToFm(wal);
        fib.close();
        return new FmTextIndex(f);
    }


    public static ChunkMetadata md(int startOffs, int typeId,
                                   long tstamp, int duration, boolean errorFlag,
                                   long dataOffs, long traceId1, long traceId2,
                                   long parentId, long spanId, int chunkNum) {

        ChunkMetadata tm = new ChunkMetadata(traceId1, traceId2, parentId, spanId, chunkNum);

        tm.setStartOffs(startOffs);
        tm.setTstamp(tstamp);
        tm.setDuration(duration);
        tm.setDataOffs(dataOffs);
        tm.setChunkNum(chunkNum);
        tm.setErrorFlag(errorFlag);

        return tm;
    }


    public ChunkMetadata md(long traceId1, long traceId2, long parentId, long spanId, int chunkNum) {
        return new ChunkMetadata(traceId1, traceId2, parentId, spanId, chunkNum);
    }


    private TraceRecord filter(TraceRecord tr, StructuredTextIndex f) {
        tr.setMethod(f.resolve(tr.getMid()));
        return tr;
    }


    public RecursiveTraceDataRetriever<TraceRecord> rtr() {
        return new RecursiveTraceDataRetriever<>(this::filter);
    }


    public static <T> Set<T> drain(Iterable<T> coll) {
        Set<T> rslt = new HashSet<>();
        for (T itm : coll) rslt.add(itm);
        return rslt;
    }
}
