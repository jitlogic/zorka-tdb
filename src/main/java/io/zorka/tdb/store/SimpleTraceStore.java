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
import io.zorka.tdb.text.StructuredTextIndex;
import io.zorka.tdb.text.CachingTextIndex;
import io.zorka.tdb.text.ci.CompositeIndex;
import io.zorka.tdb.text.ci.CompositeIndexFileStore;
import io.zorka.tdb.util.BitmapSet;
import io.zorka.tdb.util.CborBufReader;

import io.zorka.tdb.util.ZicoUtil;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;

import static io.zorka.tdb.store.ChunkMetadata.BOOL_TYPE;
import static io.zorka.tdb.store.ChunkMetadata.INT_TYPE;

/**
 *
 */
public class SimpleTraceStore implements TraceStore {

    private static final Logger log = LoggerFactory.getLogger(SimpleTraceStore.class);

    /** Read only index, old WALs are packed to FM format, no new WALs are created. */
    public final static int CTF_ARCHIVED = 0x01;

    private File baseDir, root;

    private long storeId;

    private volatile StructuredTextIndex itext;
    private volatile CompositeIndex ctext;
    private volatile RawTraceDataFile fdata;
    private volatile DB db;

    public static final long ERROR_BIT = 0x8000000000000000L;

    // Tstamp in nanoseconds, in case of conflicts it is incremented to resolve conflict

    /** tstamp -> serialized chunk. */
    private ConcurrentNavigableMap<Long,byte[]> chunks;

    /** Main index: tid1+tid2+sid+chnum -> tstamp */
    private ConcurrentNavigableMap<Fun.Tuple4<Long,Long,Long,Integer>,Long> tids;

    /** Sorted by duration timestamp: tstamp -> duration+err */
    private ConcurrentNavigableMap<Long,Long> tstamps;

    /** String attributes: keyId+valId+tstamp -> duration+err */
    private ConcurrentNavigableMap<Fun.Tuple3<Integer,Integer,Long>,Long> sattrs;

    // TODO fulltext: valId+SEQ -> keyId+duration+err

    /** Numeric/Boolean attributes: (type|keyID)+val+tstamp -> duration+err */
    private ConcurrentNavigableMap<Fun.Tuple3<Integer,Long,Long>,Long> nattrs; // Types (2-bit): bool=1,long=2,double=3

    // TODO factor out trace/agent data handling code to separate class
    private final Map<String,AgentHandler> handlers = new ConcurrentHashMap<>();

    private volatile int iFlags = 0;

    private int sessionTimeout = 90000;

    private int textCacheSize = 16384;

    private Map<String,TraceDataIndexer> indexerCache;

    private Properties props;


    public SimpleTraceStore(File root, Properties props, Map<String,TraceDataIndexer> indexerCache) {
        this.indexerCache = indexerCache;
        this.root = root;
        this.baseDir = this.root.getParentFile();

        if (!baseDir.exists()) {
            throw new ZicoException("Directory " + baseDir + " does not exist.");
        } else if (!baseDir.isDirectory()) {
            throw new ZicoException("Path " + baseDir + " is not a directory.");
        }

        this.storeId = Integer.parseInt(this.root.getName(), 16);

        if (props == null) {
            props = new Properties();
        }

        this.props = props;

        if (!baseDir.isDirectory() && !baseDir.mkdir()) {
            throw new ZicoException("Cannot create directory " + root);
        }
    }

    public synchronized Properties getProps() {
        return props;
    }


    @Override
    public synchronized void open() {

        // TODO writeLock here
        if (fdata != null) {
            return;
        }

        db = DBMaker.newFileDB(new File(root, "meta.db"))
                // TODO mmapEnable(), segment size etc.
                .closeOnJvmShutdown().cacheSize(8192).make();

        chunks = db.getTreeMap("chunks.map");
        tids = db.getTreeMap("tids.map");
        tstamps = db.getTreeMap("tstamps.map");
        sattrs = db.getTreeMap("sattrs.map");
        nattrs = db.getTreeMap("nattrs.map");

        if (db.getAtomicBoolean("archived.flag").get()) iFlags |= CTF_ARCHIVED;

        Properties ptext = ZicoUtil.props(); // TODO configure properties here
        CompositeIndexFileStore ftext = new CompositeIndexFileStore(root.getPath(), "text", ptext);
        ctext = new CompositeIndex(ftext, ptext);

        if (0 == (iFlags & CTF_ARCHIVED)) {
            itext = new StructuredTextIndex(new CachingTextIndex(ctext, textCacheSize));
        } else {
            itext = new StructuredTextIndex(ctext);
        }

        fdata = new RawTraceDataFile(new File(root, "traces.dat"), true,
            RawTraceDataFile.ZLIB_COMPRESSION | RawTraceDataFile.CRC32_CHECKSUM);
    }


    public synchronized long getStoreId() {
        return storeId;
    }


    public synchronized StructuredTextIndex getTextIndex() {
        return itext;
    }


    public synchronized RawTraceDataFile getDataFile() {
        return fdata;
    }


    public synchronized Map<String,TraceDataIndexer> getIndexerCache() {
        return indexerCache;
    }


    public ConcurrentNavigableMap<Long,Long> getTstamps() {
        return tstamps;
    }


    public ConcurrentNavigableMap<Fun.Tuple3<Integer,Integer,Long>,Long> getSattrs() {
        return sattrs;
    }


    @Override
    public long getTstart() {
        checkOpen();
        return tstamps.size() > 0 ? tstamps.firstKey() : 0L;
    }


    @Override
    public synchronized long getTstop() {
        checkOpen();
        return tstamps.size() > 0 ? tstamps.lastKey() : 0L;
    }


    private synchronized AgentHandler getHandler(String sessionId, boolean reset) {
        AgentHandler agentHandler = handlers.get(sessionId);

        if (agentHandler == null && !reset) {
            throw new ZicoException("No such session: " + sessionId);
        }

        if (agentHandler == null) {
            agentHandler = new AgentHandler(this, sessionId);
            handlers.put(sessionId, agentHandler);
        }

        return agentHandler;
    }


    public void retrieveRaw(long chunkId, ByteArrayOutputStream bos) {
        ChunkMetadata cm = getChunkMetadata(chunkId);
        CborBufReader rdr = fdata.read(cm.getDataOffs());
        try {
            bos.write(rdr.getRawBytes());
        } catch (IOException e) {
            // Ignore this
        }
    }


    private void checkOpen() {
        if (fdata == null) open();
    }


    public <T> void retrieveChunk(long chunkId, TraceDataRetriever<T> rtr) {
        retrieveChunk(chunkId, true, rtr);
    }


    <T> void retrieveChunk(long chunkId, boolean first, TraceDataRetriever<T> rtr) {
        ChunkMetadata cm = getChunkMetadata(chunkId);
        if (cm != null) {
            CborBufReader rdr = fdata.read(cm.getDataOffs());
            if (first) rdr.position(cm.getStartOffs());
            rtr.setResolver(itext);
            TraceDataReader tdr = new TraceDataReader(rdr, rtr);
            rtr.setReader(tdr);
            tdr.run();
        }
    }


    void saveChunkMetadata(ChunkMetadata cm) {

        // Ensure tstamp is unique
        long tst = cm.getTstamp();
        while (tstamps.containsKey(tst)) tst++;
        cm.setTstamp(tst);

        long dur = cm.getDuration()|(cm.hasError()?ERROR_BIT:0);
        chunks.put(tst,ChunkMetadata.serialize(cm));
        tids.put(Fun.t4(cm.getTraceId1(),cm.getTraceId2(),cm.getSpanId(),cm.getChunkNum()),tst);
        tstamps.put(tst,dur);
        if (cm.getSattrs() != null) {
            for (Map.Entry<Integer,Integer> e : cm.getSattrs().entrySet()) {
                sattrs.put(Fun.t3(e.getKey(),e.getValue(),tst),dur);
            }
        }
        if (cm.getNattrs() != null) {
            for (Map.Entry<Integer,Long> e : cm.getNattrs().entrySet()) {
                nattrs.put(Fun.t3(e.getKey(),e.getValue(),tst),dur);
            }
        }
        db.commit();
    }


    public ChunkMetadata getChunkMetadata(long tstamp) {
        if (tstamp ==-1) return null;
        checkOpen();
        byte[] b = chunks.get(tstamp);
        if (b == null) return null;
        ChunkMetadata c = ChunkMetadata.deserialize(b);
        c.setStore(this);
        return c;
    }


    int getChunks(Tid t, List<ChunkMetadata> acc) {

        checkOpen();

        int count = 0;

        Fun.Tuple4<Long, Long, Long, Integer> t1 = Fun.t4(t.t1, t.t2, t.s!= 0L?t.s:Long.MIN_VALUE, Integer.MIN_VALUE);
        Fun.Tuple4<Long, Long, Long, Integer> t2 = Fun.t4(t.t1, t.t2, (t.s!= 0L?t.s+1:Long.MAX_VALUE), Integer.MAX_VALUE);

        for (Long tst : tids.subMap(t1, t2).values()) {
            acc.add(getChunkMetadata(tst));
            count++;
        }

        return count;
    }


    void getAttributes(ChunkMetadata md, Map<String,Object> acc) {
        checkOpen();

        if (md.getSattrs() != null) {
            for (Map.Entry<Integer,Integer> e : md.getSattrs().entrySet()) {
                String k = itext.resolve(e.getKey());
                String v = itext.resolve(e.getValue());
                if (k != null && v != null) acc.put(k, v);
            }
        }

        if (md.getNattrs() != null) {
            for (Map.Entry<Integer,Long> e : md.getNattrs().entrySet()) {
                String k = itext.resolve(e.getKey() & ~ChunkMetadata.TYPE_MASK);
                if (k == null) continue;
                switch (e.getKey() & ChunkMetadata.TYPE_MASK) {
                    case INT_TYPE:
                        acc.put(k, e.getValue()); break;
                    case BOOL_TYPE:
                        acc.put(k, e.getValue() != 0); break;
                    case ChunkMetadata.DBL_TYPE:
                        acc.put(k, Double.longBitsToDouble(e.getValue())); break;
                    case ChunkMetadata.NULL_TYPE:
                        acc.put(k, null);
                }
            }
        }
    }


    int getAttributeValues(String attr, int limit, BitmapSet bs, List<String> acc) {
        checkOpen();

        int aid = itext.get(attr);
        if (aid <= 0) return 0;

        int count = 0;

        Fun.Tuple3<Integer,Integer,Long> t1 = Fun.t3(aid, Integer.MIN_VALUE, 0L);
        Fun.Tuple3<Integer,Integer,Long> t2 = Fun.t3(aid, Integer.MAX_VALUE, 0L);

        ConcurrentNavigableMap<Fun.Tuple3<Integer,Integer,Long>,Long> m1 = sattrs.subMap(t1,t2);
        if (m1.size() == 0) return 0;

        Fun.Tuple3<Integer,Integer,Long> c = m1.firstKey();

        while (count < limit && c != null) {
            int v = c.b;
            if (!bs.get(v)) {
                String s = itext.resolve(v);
                if (s != null) {
                    count++;
                    bs.set(v);
                    acc.add(s);
                }
            }
            c = m1.higherKey(Fun.t3(c.a, c.b, Long.MAX_VALUE));
        }

        return count;
    }


    @Override
    public void handleTraceData(String sessionUUID, byte[] data, ChunkMetadata md) {
        getHandler(sessionUUID, false).handleTraceData(data, md);
    }


    @Override
    public void handleAgentData(String sessionUUID, boolean reset, byte[] data) {
        AgentHandler handler = getHandler(sessionUUID, reset);
        handler.handleAgentData(data);
    }


    public synchronized void archive() {
        if (0 == (iFlags & CTF_ARCHIVED)) {
            checkOpen();
            iFlags |= CTF_ARCHIVED;
            db.getAtomicBoolean("archived.flag").set(true);
            db.commit();
            ctext.archive();
            itext = new StructuredTextIndex(ctext);
        }
    }

    public boolean runMaintenance() {
        checkOpen();
        cleanupSessions();
        return ctext.runMaintenance();
    }

    public long length() {
        return fdata.length() + ctext.length();
    }


    @Override
    public synchronized void close() throws IOException {
        ctext.close();
        fdata.close();
        db.close();
    }

    private synchronized void cleanupSessions() {
        long tst = System.currentTimeMillis();
        synchronized (handlers) {
            handlers.values().stream()
                .filter(ah -> tst - ah.getLastTstamp() > sessionTimeout)
                .map(AgentHandler::getSessionId)
                .forEach(handlers::remove);
        }
    }

    public int search(TraceSearchQuery query, int limit, int offset, List<ChunkMetadata> acc) {
        return new SimpleTraceStoreSearchContext(this, query, limit, offset).search(acc);
    }

}

