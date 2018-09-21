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

package io.zorka.tdb.store;

import io.zorka.tdb.MissingSessionException;
import io.zorka.tdb.ZicoException;
import io.zorka.tdb.meta.*;
import io.zorka.tdb.search.*;
import io.zorka.tdb.text.CachingTextIndex;
import io.zorka.tdb.text.ci.CompositeIndex;
import io.zorka.tdb.text.ci.CompositeIndexFileStore;
import io.zorka.tdb.util.CborBufReader;

import io.zorka.tdb.util.ZicoUtil;
import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.meta.MetadataTextIndex;
import io.zorka.tdb.meta.StructuredTextIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static io.zorka.tdb.store.TraceStoreUtil.*;

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

    private CompositeIndex ctext, cmeta;
    private volatile StructuredTextIndex itext;
    private MetadataTextIndex imeta;
    private MetadataQuickIndex qindex;

    private ChunkMetadataProcessor postproc;

    private volatile RawTraceDataFile fdata;

    private final Map<String,AgentHandler> handlers = new ConcurrentHashMap<>();

    private long tidxWalSize = 128;

    private long midxWalSize = 128;

    private int tidxWalNum = 1;

    private int midxWalNum = 1;

    private Executor indexerExecutor;

    private Executor cleanerExecutor;

    private int iFlags = 0;

    private int dFlags = 0;

    private int sessionTimeout = 90000;

    private int textCacheSize = 16384;

    private Map<String,TraceDataIndexer> indexerCache;

    private TraceTypeResolver traceTypeResolver;

    private Properties props;


    public SimpleTraceStore(File root, Properties props, Executor indexerExecutor, Executor cleanerExecutor,
                            Map<String,TraceDataIndexer> indexerCache, TraceTypeResolver traceTypeResolver) {
        this.indexerCache = indexerCache;
        this.root = root;
        this.baseDir = this.root.getParentFile();
        this.traceTypeResolver = traceTypeResolver;

        if (!baseDir.exists()) {
            throw new ZicoException("Directory " + baseDir + " does not exist.");
        } else if (!baseDir.isDirectory()) {
            throw new ZicoException("Path " + baseDir + " is not a directory.");
        }

        this.storeId = Integer.parseInt(this.root.getName(), 16);


        // TODO this is too complicated, get rid of all variants except for

        if (props == null) {
            props = new Properties();
        }

        configure(props, indexerExecutor, cleanerExecutor);
        configure(loadProps(), indexerExecutor, cleanerExecutor);


        if (!baseDir.isDirectory() && !baseDir.mkdir()) {
            throw new ZicoException("Cannot create directory " + root);
        }
    }

    public Properties getProps() {
        return props;
    }

    private Properties loadProps() {

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


    private void saveProps() {
        Properties props = new Properties();

        props.setProperty(ConfigProps.TIDX_WAL_SIZE, ""+tidxWalSize);
        props.setProperty(ConfigProps.TIDX_WAL_NUM, ""+tidxWalNum);
        props.setProperty(ConfigProps.MIDX_WAL_SIZE, ""+midxWalSize);
        props.setProperty(ConfigProps.MIDX_WAL_NUM, ""+midxWalNum);

        props.setProperty(ConfigProps.IFLAGS, ""+ iFlags);
        props.setProperty(ConfigProps.DFLAGS, ""+ dFlags);

        try (FileOutputStream fos = new FileOutputStream(new File(root, PROPS_FILE))) {
            props.store(fos, "ZicoDB store properties saved at " + new Date());
        } catch (IOException e) {
            log.error("Cannot save store properties for " + root, e);
        }
    }


    @Override
    public void configure(Properties props, Executor indexerExecutor, Executor cleanerExecutor) {
        this.props = props;
        this.indexerExecutor = indexerExecutor;
        this.cleanerExecutor = cleanerExecutor;
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

        if (fdata != null) {
            return;
        }

        Properties ptext = ZicoUtil.props(); // TODO configure properties here
        CompositeIndexFileStore ftext = new CompositeIndexFileStore(root.getPath(), "text", ptext);
        ctext = new CompositeIndex(ftext, ptext, indexerExecutor);

        if (0 == (iFlags & CTF_ARCHIVED)) {
            itext = new StructuredTextIndex(new CachingTextIndex(ctext, textCacheSize));
        } else {
            itext = new StructuredTextIndex(ctext);
        }

        Properties pmeta = ZicoUtil.props(); // TODO configure properties here
        CompositeIndexFileStore fmeta = new CompositeIndexFileStore(root.getPath(), "meta", pmeta);
        cmeta = new CompositeIndex(fmeta, pmeta, indexerExecutor);

        imeta = new MetadataTextIndex(cmeta);

        qindex = new MetadataQuickIndex(new File(root, "qmeta.idx"));

        fdata = new RawTraceDataFile(new File(root, "traces.dat"), true,
            RawTraceDataFile.ZLIB_COMPRESSION | RawTraceDataFile.CRC32_CHECKSUM);
    }

    public long getStoreId() {
        return storeId;
    }

    public StructuredTextIndex getTextIndex() {
        return itext;
    }

    public MetadataTextIndex getMetaIndex() {
        return imeta;
    }

    public MetadataQuickIndex getQuickIndex() {
         return qindex;
    }

    public RawTraceDataFile getDataFile() {
        return fdata;
    }

    public Map<String,TraceDataIndexer> getIndexerCache() {
        return indexerCache;
    }

    public ChunkMetadataProcessor getPostproc() {
        return postproc;
    }

    public void setPostproc(ChunkMetadataProcessor postproc) {
        this.postproc = postproc;
    }

    @Override
    public long getTstart() {
        if (fdata == null) open();

        return qindex.getTstart();
    }

    @Override
    public long getTstop() {
        if (fdata == null) open();

        return qindex.getTstop();
    }

    private AgentHandler getHandler(String sessionUUID, String agentUUID) {
        synchronized (handlers) {
            AgentHandler agentHandler = handlers.get(agentUUID);
            if (agentHandler == null || !agentHandler.getSessionUUID().equals(sessionUUID)) {
                throw new MissingSessionException(sessionUUID, agentUUID);
            }
            return agentHandler;
        }
    }

    public void retrieveRaw(long chunkId, ByteArrayOutputStream bos) {
        if (fdata == null) open();

        int slotId = parseSlotId(chunkId);
        long offs = qindex.getDataOffs(slotId);
        CborBufReader rdr = fdata.read(offs);
        try {
            bos.write(rdr.getRawBytes());
        } catch (IOException e) {
            // Ignore this
        }
    }

    public <T> void retrieveChunk(long chunkId, TraceDataRetriever<T> rtr) {
        retrieveChunk(chunkId, true, rtr);
    }

    public <T> void retrieveChunk(long chunkId, boolean first, TraceDataRetriever<T> rtr) {
        if (fdata == null) open();

        int slotId = parseSlotId(chunkId);
        ChunkMetadata md = qindex.getChunkMetadata(slotId);
        CborBufReader rdr = fdata.read(md.getDataOffs());
        if (first) rdr.position(md.getStartOffs());
        rtr.setResolver(itext);
        TraceDataReader tdr = new TraceDataReader(rdr, rtr);
        rtr.setReader(tdr);
        tdr.run();
    }


    @Override
    public String getTraceUUID(long chunkId) {
        if (fdata == null) open();

        return qindex.getChunkUUID(TraceStoreUtil.parseSlotId(chunkId));
    }

    @Override
    public long getTraceDuration(long chunkId) {
        if (fdata == null) open();

        return qindex.getTraceDuration(TraceStoreUtil.parseSlotId(chunkId));
    }

    @Override
    public String getDesc(long chunkId) {
        if (fdata == null) open();

        int did = qindex.getDid(TraceStoreUtil.parseSlotId(chunkId));

        return did > 0 ? itext.resolve(did) : null;
    }


    @Override
    public List<Long> getChunkIds(String traceUUID) {
        if (fdata == null) open();

        List<Long> rslt = new ArrayList<>();
        qindex.findChunkIds(rslt, (int)storeId, UUID.fromString(traceUUID));

        return rslt;
    }

    public void findChunkIds(List<Long> acc, UUID uuid) {
        if (fdata == null) open();

        qindex.findChunkIds(acc, (int)storeId, uuid);
    }

    @Override
    public ChunkMetadata getChunkMetadata(long chunkId) {
        if (fdata == null) open();

        if (chunkId ==-1) return null;
        int slotId = parseSlotId(chunkId);
        return qindex.getChunkMetadata(slotId);
    }


    @Override
    public void handleTraceData(String agentUUID, String sessionUUID, String traceUUID, String data, ChunkMetadata md) {
        getHandler(sessionUUID, agentUUID).handleTraceData(traceUUID, data, md);
    }


    public void handleAgentData(String agentUUID, String sessionUUID, String data) {
        getHandler(sessionUUID, agentUUID).handleAgentData(data);
    }


    @Override
    public synchronized void archive() {
        if (0 == (iFlags & CTF_ARCHIVED)) {
            if (fdata == null) open();
            iFlags |= CTF_ARCHIVED;
            ctext.archive();
            cmeta.archive();
            qindex.archive();
            itext = new StructuredTextIndex(ctext);
        }
    }

    @Override
    public synchronized boolean runMaintenance() {
        if (fdata == null) open();

        cleanupSessions();

        return ctext.runMaintenance() || cmeta.runMaintenance();
    }

    @Override
    public synchronized String getSession(String agentUUID) {
        AgentHandler handler = handlers.get(agentUUID);
        if (handler != null) {
            return handler.getSessionUUID();
        } else {
            String sessionUUID = UUID.randomUUID().toString();
            handler = new AgentHandler(this, agentUUID, sessionUUID, traceTypeResolver);
            handlers.put(agentUUID, handler);
            return sessionUUID;
        }
    }


    public long length() {
        return fdata.length() + ctext.length() + cmeta.length();
    }


    @Override
    public void close() throws IOException {
        ctext.close();
        cmeta.close();
        fdata.close();
        qindex.close();
        saveProps();
    }

    // TODO implement session timeout & cleanup functionality

    private void cleanupSessions() {

        long tst = System.currentTimeMillis();

        synchronized (handlers) {
            handlers.values().stream()
                .filter(ah -> tst - ah.getLastTstamp() > sessionTimeout)
                .map(AgentHandler::getSessionUUID)
                .forEach(handlers::remove);
        }
    }

    public static int SEARCH_QR_THRESHOLD = 512;
    public static int SEARCH_TX_THRESHOLD = 512;

    public long toChunkId(int slot) {
        return slot >= 0 ? (((long)storeId) << 32) | slot : -1;
    }


    @Override
    public TraceSearchResult searchTraces(TraceSearchQuery query) {
        return new SimpleTraceStoreSearchResult(query, this);
    }


} // class SimpleTraceStore { .. }

