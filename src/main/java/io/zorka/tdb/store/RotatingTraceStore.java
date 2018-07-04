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

import io.zorka.tdb.ZicoException;
import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.search.SearchNode;
import io.zorka.tdb.search.SearchableStore;
import io.zorka.tdb.search.rslt.ListSearchResultsMapper;
import io.zorka.tdb.search.rslt.StreamingSearchResult;
import io.zorka.tdb.text.ci.CompositeIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static io.zorka.tdb.store.ConfigProps.*;
import static io.zorka.tdb.store.TraceStoreUtil.*;

/**
 *
 */
public class RotatingTraceStore implements TraceStore, SearchableStore {

    private static final Logger log = LoggerFactory.getLogger(RotatingTraceStore.class);

    private File baseDir;

    private ConcurrentNavigableMap<Integer,SimpleTraceStore> archived = new ConcurrentSkipListMap<>();

    private volatile SimpleTraceStore current;

    private int currentId;

    /** WAL->FMI compression/indexing tasks are executed by this executor. */
    private Executor indexerExecutor;

    /** Cleanup tasks are executed by this executor. */
    private Executor cleanerExecutor;

    private Properties props;

    private int storesMax;

    private long storeSize;

    private ReadWriteLock rwlock = new ReentrantReadWriteLock(true);

    private Lock rdlock = rwlock.readLock(), wrlock = rwlock.writeLock();

    private Map<String,TraceDataIndexer> indexerCache;

    private volatile ChunkMetadataProcessor postproc = null;

    private TraceTypeResolver traceTypeResolver;

    public RotatingTraceStore(File baseDir, Properties props, TraceTypeResolver traceTypeResolver,
                              Executor indexerExecutor, Executor cleanerExecutor,
                              Map<String,TraceDataIndexer> indexerCache) {
        this.baseDir = baseDir;

        configure(props, indexerExecutor, cleanerExecutor);

        this.indexerCache = indexerCache;
        this.traceTypeResolver = traceTypeResolver;

        if (!baseDir.exists() || !baseDir.isDirectory()) {
            throw new ZicoException("Path " + baseDir + " does not exist or is not a directory.");
        }

        open();
    }

    @Override
    public void configure(Properties props, Executor indexerExecutor, Executor cleanerExecutor) {
        this.props = props;

        this.indexerExecutor = indexerExecutor;
        this.cleanerExecutor = cleanerExecutor;

        this.storeSize = Integer.parseInt(props.getProperty(STORES_MAX_SIZE, "16"));    // default 16GB
        this.storesMax = Integer.parseInt(props.getProperty(STORES_MAX_NUM, "16"));     // default 256GB

        for (Map.Entry<Integer,SimpleTraceStore> e : archived.entrySet()) {
            e.getValue().configure(props, indexerExecutor, cleanerExecutor);
        }

    }

    @Override
    public void setPostproc(ChunkMetadataProcessor postproc) {
        this.postproc = postproc;
        current.setPostproc(postproc);
    }

    @Override
    public long getTstart() {
        if (archived.firstEntry() != null) {
            return archived.firstEntry().getValue().getTstart();
        }
        return current.getTstart();
    }

    @Override
    public long getTstop() {
        return current.getTstop();
    }

    public Map<String, TraceDataIndexer> getIndexerCache() {
        return indexerCache;
    }

    @Override
    public synchronized void open() {

        log.info("Opening store at " + baseDir);

        List<File> stores = Arrays.stream(baseDir.list())
            .filter(f -> f.matches("[0-9a-f]{6}"))
            .map(f -> new File(baseDir, f))
            .filter(File::isDirectory)
            .sorted()
            .collect(Collectors.toList());

        if (log.isDebugEnabled()) {
            log.debug("Found store parts: " + stores);
        }

        if (stores.isEmpty()) {
            File d = new File(baseDir, "000000");
            if (!d.exists()) {
                d.mkdirs();
            }
            stores.add(d);
        }

        File cf = stores.get(stores.size()-1);

        current = new SimpleTraceStore(cf, props, indexerExecutor, cleanerExecutor, indexerCache, traceTypeResolver);
        current.open();
        currentId = Integer.parseInt(cf.getName(), 16);

        for (File af : stores.subList(0, stores.size()-1)) {
            int id = Integer.parseInt(af.getName(), 16);
            archived.put(id, new SimpleTraceStore(af, props, indexerExecutor, cleanerExecutor, indexerCache, traceTypeResolver));
        }
    }


    public byte[] retrieveRaw(String traceUUID) {
        List<Long> chunkIds = getChunkIds(traceUUID);
        if (chunkIds.size() == 0) return null;
        chunkIds.sort(Comparator.comparingLong(o -> o & SLOT_MASK));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        for (Long chunkId : chunkIds) {
            int storeId = parseStoreId(chunkId);
            SimpleTraceStore sts = storeId == currentId ? current : archived.get(storeId);
            sts.retrieveRaw(chunkId, bos);
        }

        return bos.toByteArray();
    }

    public <T> T retrieve(String traceUUID, TraceDataRetriever<T> rtr) {

        List<Long> chunkIds = getChunkIds(traceUUID);

        for (long chunkId : chunkIds) {
            int storeId = parseStoreId(chunkId);
            SimpleTraceStore ts = storeId == currentId ? current : archived.get(storeId);
            ts.open();
            ts.retrieveChunk(chunkId, rtr);
        }

        rtr.commit();
        return rtr.getResult();
    }


    @Override
    public String getTraceUUID(long chunkId) {
        int sid = parseStoreId(chunkId);
        SimpleTraceStore ts = sid == currentId ? current : archived.get(sid);
        return ts != null ? ts.getTraceUUID(chunkId) : null;
    }

    @Override
    public long getTraceDuration(long chunkId) {
        int sid = parseStoreId(chunkId);
        SimpleTraceStore ts = sid == currentId ? current : archived.get(sid);
        return ts != null ? ts.getTraceDuration(chunkId) : -1;
    }


    @Override
    public String getDesc(long chunkId) {
        int sid = parseStoreId(chunkId);
        SimpleTraceStore ts = sid == currentId ? current : archived.get(sid);
        return ts != null ? ts.getDesc(chunkId) : null;
    }


    @Override
    public List<Long> getChunkIds(String traceUUID) {
        List<Long> rslt = current.getChunkIds(traceUUID);

        int storeId = currentId - 1;
        UUID uuid = UUID.fromString(traceUUID);

        while (!containsStartChunk(rslt) && archived.containsKey(storeId)) {
            archived.get(storeId--).findChunkIds(rslt, uuid);
        }

        rslt.sort(Comparator.comparingLong(o -> o & (CH_SE_MASK)));

        return rslt;
    }


    @Override
    public ChunkMetadata getChunkMetadata(long chunkId) {
        if (chunkId == -1) return null;
        int sid = parseStoreId(chunkId);
        TraceStore ts = sid == currentId ? current : archived.get(sid);
        return ts != null ? ts.getChunkMetadata(chunkId) : null;
    }


    @Override
    public void archive() {
        rotate();
    }

    @Override
    public synchronized boolean runMaintenance() {
        boolean rslt = false;

        TraceStore cur = current;

        if (cur != null) {
            rslt = cur.runMaintenance();
        }

        for (Map.Entry<Integer,SimpleTraceStore> e : archived.entrySet()) {
            rslt = rslt || e.getValue().runMaintenance();
        }

        return rslt;
    }


    @Override
    public String getSession(String agentUUID) {
        return current.getSession(agentUUID);
    }


    @Override
    public void handleTraceData(String agentUUID, String sessionUUID, String traceUUID, String data, ChunkMetadata md) {

        if (log.isDebugEnabled()) {
            log.debug("Got trace data from " + agentUUID + " (" + sessionUUID + ")");
        }

        if (current.length() > storeSize * CompositeIndex.MB) {
            rotate();
        }

        try {
            rdlock.lock();
            current.handleTraceData(agentUUID, sessionUUID, traceUUID, data, md);
        } finally {
            rdlock.unlock();
        }
    }


    @Override
    public void handleAgentData(String agentUUID, String sessionUUID, String data) {

        if (log.isDebugEnabled()) {
            log.debug("Got agent state from " + agentUUID + " (" + sessionUUID + ")");
        }

        if (current.length() > storeSize * CompositeIndex.MB) {
            rotate();
        }

        try {
            rdlock.lock();
            current.handleAgentData(agentUUID, sessionUUID, data);
        } finally {
            rdlock.unlock();
        }
    }


    /**
     * Archives current log and opens next one.
     */
    private synchronized void rotate() {
        try {
            log.info("Archiving store " + String.format("%06x", currentId) + " in " + baseDir);
            wrlock.lock();
            archived.put(currentId, current);
            currentId++;
            File root = new File(baseDir, String.format("%06x", currentId));
            if (!root.exists()) {
                if (!root.mkdirs()) {
                    throw new ZicoException("Cannot create directory: " + root);
                }
            }
            current = new SimpleTraceStore(root, props, indexerExecutor, cleanerExecutor, indexerCache, traceTypeResolver);
            current.open();
            current.setPostproc(postproc);
            archived.get(currentId - 1).archive();
        } finally {
            wrlock.unlock();
        }
    }


    @Override
    public void close() throws IOException {
        log.info("Closing rotating trace store: " + baseDir);
        for (Map.Entry<Integer,SimpleTraceStore> e : archived.entrySet()) {
            e.getValue().close();
        }
        current.close();
    }


    private synchronized List<TraceStore> listStores() {
        List<TraceStore> lst = new ArrayList<>();
        lst.add(current);
        lst.addAll(archived.values());
        return lst;
    }


    @Override
    public io.zorka.tdb.search.rslt.SearchResult search(SearchNode expr) {
        ListSearchResultsMapper<TraceStore> results = new ListSearchResultsMapper<>(
                listStores(), store -> store.search(expr));
        return new StreamingSearchResult(results);
    }

    public SimpleTraceStore getCurrent() {
        return current;
    }
} // class RotatingTraceStore { .. }
