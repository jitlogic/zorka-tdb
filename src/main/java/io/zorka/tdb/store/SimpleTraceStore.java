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
import io.zorka.tdb.search.TraceSearchQuery;
import io.zorka.tdb.text.CachingTextIndex;
import io.zorka.tdb.text.ci.CompositeIndex;
import io.zorka.tdb.text.ci.CompositeIndexFileStore;
import io.zorka.tdb.util.CborBufReader;

import io.zorka.tdb.util.ZicoUtil;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 *
 */
public class SimpleTraceStore implements TraceStore {

    private static final Logger log = LoggerFactory.getLogger(SimpleTraceStore.class);

    private static final String PROPS_FILE = "store.props";

    /** Read only index, old WALs are packed to FM format, no new WALs are created. */
    public final static int CTF_ARCHIVED = 0x01;

    private File baseDir, root;

    private long storeId;

    private volatile StructuredTextIndex itext;
    private volatile CompositeIndex ctext;
    private volatile RawTraceDataFile fdata;
    private volatile DB db;

    /** Next sequence number */
    private Atomic.Integer nextSeq;

    /** SEQ -> serialized chunk. */
    private ConcurrentNavigableMap<Integer,byte[]> chunks;

    /** Main index: tid1+tid2+sid+chnum -> SEQ */
    private ConcurrentNavigableMap<Fun.Tuple4<Long,Long,Long,Integer>,Integer> tids;

    /** Sorted by duration timestamp: tstamp+SEQ -> duration+err */
    private ConcurrentNavigableMap<Fun.Tuple2<Long,Integer>,Long> tstamps;

    /** String attributes: keyId+valId+SEQ -> duration+err */
    private ConcurrentNavigableMap<Fun.Tuple3<Integer,Integer,Integer>,Long> sattrs;

    // TODO fulltext: valId+SEQ -> keyId+duration+err

    /** Numeric/Boolean attributes: (type|keyID)+val+SEQ -> duration+err */
    private ConcurrentNavigableMap<Fun.Tuple3<Integer,Long,Integer>,Long> nattrs; // Types (2-bit): bool=1,long=2,double=3

    // TODO factor out trace/agent data handling code to separate class
    private final Map<String,AgentHandler> handlers = new ConcurrentHashMap<>();

    private long tidxWalSize = 128;
    private long midxWalSize = 128;
    private int tidxWalNum = 1;
    private int midxWalNum = 1;

    private volatile int iFlags = 0;

    private volatile int dFlags = 0;

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

        configure(props);
        configure(loadProps());

        if (!baseDir.isDirectory() && !baseDir.mkdir()) {
            throw new ZicoException("Cannot create directory " + root);
        }
    }

    public synchronized Properties getProps() {
        return props;
    }

    private synchronized Properties loadProps() {

        File f = new File(root, PROPS_FILE);

        if (f.canRead() && f.isFile()) {
            try (FileInputStream fis = new FileInputStream(f)) {
                props.load(fis);
            } catch (IOException e) {
                log.error("Cannot load store properties for " + root, e);
            }
        }

        return props;
    }


    public synchronized void saveProps() {
        Properties p = new Properties();

        for (String n : props.stringPropertyNames()) {
            p.setProperty(n, props.getProperty(n));
        }

        p.setProperty(ConfigProps.TIDX_WAL_SIZE, ""+tidxWalSize);
        p.setProperty(ConfigProps.TIDX_WAL_NUM, ""+tidxWalNum);
        p.setProperty(ConfigProps.MIDX_WAL_SIZE, ""+midxWalSize);
        p.setProperty(ConfigProps.MIDX_WAL_NUM, ""+midxWalNum);

        p.setProperty(ConfigProps.IFLAGS, ""+ iFlags);
        p.setProperty(ConfigProps.DFLAGS, ""+ dFlags);

        try (FileOutputStream fos = new FileOutputStream(new File(root, PROPS_FILE))) {
            p.store(fos, "ZicoDB store properties saved at " + new Date());
        } catch (IOException e) {
            log.error("Cannot save store properties for " + root, e);
        }
    }


    public synchronized void configure(Properties props) {
        this.props = props;
        for (Map.Entry<Object,Object> e : props.entrySet()) {
            switch ((String)e.getKey()) {
                case ConfigProps.TIDX_WAL_SIZE:
                    tidxWalSize = Integer.parseInt((String)e.getValue());
                    break;
                case ConfigProps.TIDX_WAL_NUM:
                    tidxWalNum = Integer.parseInt((String)e.getValue());
                    break;
                case ConfigProps.MIDX_WAL_SIZE:
                    midxWalSize = Integer.parseInt((String)e.getValue());
                    break;
                case ConfigProps.MIDX_WAL_NUM:
                    midxWalNum = Integer.parseInt((String)e.getValue());
                    break;
                case ConfigProps.IFLAGS:
                    iFlags = Integer.parseInt((String)e.getValue());
                    break;
                case ConfigProps.DFLAGS:
                    dFlags = Integer.parseInt((String)e.getValue());
                    break;
            }
        }
    }


    public synchronized void open() {

        // TODO writeLock here
        if (fdata != null) {
            return;
        }

        Properties ptext = ZicoUtil.props(); // TODO configure properties here
        CompositeIndexFileStore ftext = new CompositeIndexFileStore(root.getPath(), "text", ptext);
        ctext = new CompositeIndex(ftext, ptext);

        if (0 == (iFlags & CTF_ARCHIVED)) {
            itext = new StructuredTextIndex(new CachingTextIndex(ctext, textCacheSize));
        } else {
            itext = new StructuredTextIndex(ctext);
        }

        db = DBMaker.newFileDB(new File(root, "meta.db"))
                // TODO mmapEnable(), segment size etc.
                .closeOnJvmShutdown().cacheSize(8192).make();

        nextSeq = db.getAtomicInteger("seq");
        chunks = db.getTreeMap("chunks.map");
        tids = db.getTreeMap("tids.map");
        tstamps = db.getTreeMap("tstamps.map");
        sattrs = db.getTreeMap("sattrs.map");
        nattrs = db.getTreeMap("nattrs.map");

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

    ConcurrentNavigableMap<Fun.Tuple4<Long, Long, Long, Integer>, Integer> getTids() {
        return tids;
    }

    ConcurrentNavigableMap<Fun.Tuple2<Long, Integer>, Long> getTstamps() {
        return tstamps;
    }


    @Override
    public long getTstart() {
        checkOpen();
        return tstamps.size() > 0 ? tstamps.firstKey().a : 0L;
    }

    @Override
    public synchronized long getTstop() {
        checkOpen();
        return tstamps.size() > 0 ? tstamps.lastKey().a : 0L;
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

    public <T> void retrieveChunk(long chunkId, boolean first, TraceDataRetriever<T> rtr) {
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

    public void saveChunkMetadata(ChunkMetadata cm) {
        int seq = nextSeq.incrementAndGet();
        long dur = cm.getDuration()|(cm.isErrorFlag()?0x8000000000000000L:0);
        chunks.put(seq,ChunkMetadata.serialize(cm));
        tids.put(Fun.t4(cm.getTraceId1(),cm.getTraceId2(),cm.getSpanId(),cm.getChunkNum()),seq);
        tstamps.put(Fun.t2(cm.getTstamp(),seq),dur);
        if (cm.getSattrs() != null) {
            for (Map.Entry<Integer,Integer> e : cm.getSattrs().entrySet()) {
                sattrs.put(Fun.t3(e.getKey(),e.getValue(),seq),dur);
            }
        }
        if (cm.getNattrs() != null) {
            for (Map.Entry<Integer,Long> e : cm.getNattrs().entrySet()) {
                nattrs.put(Fun.t3(e.getKey(),e.getValue(),seq),dur);
            }
        }
        db.commit();
    }


    @Override
    public long getTraceDuration(long chunkId) {
        return getChunkMetadata(chunkId).getDuration();
    }


    @Override
    public String getDesc(long chunkId) {
        return "NIECZYNNE";
    }


    @Override
    public List<Long> getChunkIds(long traceId1, long traceId2, long spanId) {
        List<Long> rslt = new ArrayList<>();
        findChunkIds(rslt, traceId1, traceId2, spanId);
        return rslt;
    }


    public void findChunkIds(List<Long> acc, long traceId1, long traceId2, long spanId) {
        checkOpen();

        ConcurrentNavigableMap<Fun.Tuple4<Long,Long,Long,Integer>,Integer> t =
                tids.subMap(Fun.t4(traceId1,traceId2,spanId,0),Fun.t4(traceId1,traceId2,spanId,Integer.MAX_VALUE));

        for (Map.Entry<Fun.Tuple4<Long,Long,Long,Integer>,Integer> e : t.entrySet()) {
            acc.add(e.getValue().longValue());
        }
    }


    @Override
    public ChunkMetadata getChunkMetadata(long chunkId) {
        if (chunkId ==-1) return null;
        checkOpen();
        int seq = TraceStoreUtil.parseSlotId(chunkId);
        byte[] b = chunks.get(seq);
        if (b == null) return null;
        return ChunkMetadata.deserialize(b);
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


    @Override
    public synchronized void archive() {
        if (0 == (iFlags & CTF_ARCHIVED)) {
            checkOpen();
            iFlags |= CTF_ARCHIVED;
            ctext.archive();
            itext = new StructuredTextIndex(ctext);
            saveProps();
        }
    }

    @Override
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
        saveProps();
    }

    // TODO implement session timeout & cleanup functionality


    private synchronized void cleanupSessions() {
        long tst = System.currentTimeMillis();
        synchronized (handlers) {
            handlers.values().stream()
                .filter(ah -> tst - ah.getLastTstamp() > sessionTimeout)
                .map(AgentHandler::getSessionId)
                .forEach(handlers::remove);
        }
    }


    public static int SEARCH_QR_THRESHOLD = 512;
    public static int SEARCH_TX_THRESHOLD = 512;


    public long toChunkId(int slot) {
        return slot >= 0 ? (storeId << 32) | slot : -1;
    }


    @Override
    public TraceSearchResult searchTraces(TraceSearchQuery query) {
        return new SimpleTraceStoreSearchResult(query, this);
    }

}

