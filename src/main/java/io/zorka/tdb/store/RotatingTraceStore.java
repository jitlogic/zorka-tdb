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
import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.search.TraceSearchQuery;
import io.zorka.tdb.text.ci.CompositeIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static io.zorka.tdb.store.ConfigProps.*;
import static io.zorka.tdb.store.RotatingTraceStoreState.*;
import static io.zorka.tdb.store.TraceStoreUtil.*;

/**
 *
 */
public class RotatingTraceStore implements TraceStore {

    private static final Logger log = LoggerFactory.getLogger(RotatingTraceStore.class);

    private File baseDir;

    private volatile RotatingTraceStoreState state = EMPTY;

    private Properties props;

    private int storesMax;

    private long storeSize;

    private Map<String,TraceDataIndexer> indexerCache;

    private TraceTypeResolver traceTypeResolver;



    public RotatingTraceStore(File baseDir, Properties props, TraceTypeResolver traceTypeResolver,
                              Map<String,TraceDataIndexer> indexerCache) {
        this.baseDir = baseDir;

        this.props = props;


        this.storeSize = Integer.parseInt(props.getProperty(STORES_MAX_SIZE, "16"));    // default 16GB
        this.storesMax = Integer.parseInt(props.getProperty(STORES_MAX_NUM, "16"));     // default 256GB

        this.indexerCache = indexerCache;
        this.traceTypeResolver = traceTypeResolver;

        if (!baseDir.exists() || !baseDir.isDirectory()) {
            throw new ZicoException("Path " + baseDir + " does not exist or is not a directory.");
        }
    }

    public synchronized void configure(Properties props) {
        checkOpen();
        this.props = props;
        SimpleTraceStore s = state.getCurrent();
        if (s != null) s.configure(props);
    }

    @Override
    public long getTstart() {
        SimpleTraceStore s = state.oldest();
        return s != null ? s.getTstart() : 0;
    }

    @Override
    public long getTstop() {
        SimpleTraceStore s = state.newest();
        return s != null ? s.getTstop() : 0;
    }

    @Override
    public TraceSearchResult searchTraces(TraceSearchQuery query) {

        checkOpen();

        RotatingTraceStoreState ts = state;

        // TODO pass state directly to RotatingTraceStoreSearchResult
        List<SimpleTraceStore> stores = new ArrayList<>(ts.getArchived().size() + 2);

        stores.add(ts.getCurrent());
        stores.addAll(ts.getArchived());
        stores.sort( (a,b) -> (int)(a.getStoreId() - b.getStoreId()));

        return new RotatingTraceStoreSearchResult(query, stores);
    }

    @Override
    public synchronized void open() {

        if (state.isOpen()) return;

        log.info("Opening store at " + baseDir);


        List<File> sdirs = Arrays.stream(baseDir.list())
            .filter(f -> f.matches("[0-9a-f]{6}"))
            .map(f -> new File(baseDir, f))
            .filter(File::isDirectory)
            .sorted()
            .collect(Collectors.toList());

        if (log.isDebugEnabled()) {
            log.debug("Found store parts: " + sdirs);
        }

        if (sdirs.isEmpty()) {
            File d = new File(baseDir, "000000");
            if (!d.exists()) {
                d.mkdirs();
            }
            sdirs.add(d);
        }

        List<SimpleTraceStore> stores = new ArrayList<>(sdirs.size()+1);

        for (File af : sdirs) {
            stores.add(new SimpleTraceStore(af, props, indexerCache, traceTypeResolver));
        }

        RotatingTraceStoreState ts = RotatingTraceStoreState.init(stores);
        ts.getCurrent().open();

        synchronized (this) {
            this.state = ts;
        }
    }


    public byte[] retrieveRaw(long traceId1, long traceId2, long spanId) {
        RotatingTraceStoreState ts = state;
        List<Long> chunkIds = getChunkIds(ts, traceId1, traceId2, spanId);

        if (chunkIds.isEmpty()) return null;

        chunkIds.sort(Comparator.comparingLong(o -> o & SLOT_MASK));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        for (Long chunkId : chunkIds) {
            SimpleTraceStore s = ts.get(parseStoreId(chunkId));
            if (s != null) {
                s.retrieveRaw(chunkId, bos);
            } else {
                // Incomplete trace will not be accessible
                return null;
            }
        }

        return bos.toByteArray();
    }


    public <T> T retrieve(long traceId1, long traceId2, long spanId, TraceDataRetriever<T> rtr) {

        checkOpen();

        RotatingTraceStoreState ts = state;

        List<Long> chunkIds = getChunkIds(ts, traceId1, traceId2, spanId);

        for (int i = 0; i < chunkIds.size(); i++) {
            long chunkId = chunkIds.get(i);
            SimpleTraceStore s = ts.get(parseStoreId(chunkId));
            if (s != null) {
                s.retrieveChunk(chunkId, i == 0, rtr);
            } else {
                // Incomplete trace will not be accessible
                return null;
            }
        }

        rtr.commit();
        return rtr.getResult();
    }


    @Override
    public long getTraceDuration(long chunkId) {
        SimpleTraceStore s = state.get(parseStoreId(chunkId));
        return s != null ? s.getTraceDuration(chunkId) : -1;
    }


    @Override
    public String getDesc(long chunkId) {
        int sid = parseStoreId(chunkId);
        SimpleTraceStore ts = state.get(sid);
        return ts != null ? ts.getDesc(chunkId) : null;
    }


    @Override
    public List<Long> getChunkIds(long traceId1, long traceId2, long spanId) {
        checkOpen();
        return getChunkIds(state, traceId1, traceId2, spanId);
    }

    private List<Long> getChunkIds(RotatingTraceStoreState state, long traceId1, long traceId2, long spanId) {
        List<Long> rslt = state.getCurrent().getChunkIds(traceId1, traceId2, spanId);

        List<SimpleTraceStore> as = state.getArchived();

        for (int i = as.size() - 1; i >= 0 && !containsStartChunk(rslt); i--) {
            as.get(i).findChunkIds(rslt, traceId1, traceId2, spanId);
        }

        rslt.sort(Comparator.comparingLong(o -> o & (CH_SE_MASK)));

        return rslt;
    }


    @Override
    public ChunkMetadata getChunkMetadata(long chunkId) {
        return getChunkMetadata(state, chunkId);
    }


    public ChunkMetadata getChunkMetadata(RotatingTraceStoreState state, long chunkId) {
        TraceStore s = state.get(parseStoreId(chunkId));
        return s != null ? s.getChunkMetadata(chunkId) : null;
    }


    @Override
    public void archive() {
        rotate();
    }

    @Override
    public boolean runMaintenance() {
        boolean rslt;

        checkOpen();

        RotatingTraceStoreState ts = state;

        rslt = ts.getCurrent().runMaintenance();

        for (SimpleTraceStore s : ts.getArchived())
            rslt |= s.runMaintenance();

        return rslt;
    }


    @Override
    public void handleTraceData(String sessionUUID, byte[] data, ChunkMetadata md) {

        if (log.isDebugEnabled()) {
            log.debug("Got trace data from for session " + sessionUUID);
        }

        checkRotate();

        // Single store is big, so it takes at least several minutes to overflow again,
        // so no practical chance of race condition here
        // TODO proper impl
        state.getCurrent().handleTraceData(sessionUUID, data, md);
    }


    @Override
    public void handleAgentData(String sessionId, boolean reset, byte[] data) {

        if (log.isDebugEnabled()) {
            log.debug("Got agent state for session " + sessionId + " (reset=" + reset + ")");
        }


        checkRotate();

        state.getCurrent().handleAgentData(sessionId, reset, data);
    }

    private void checkOpen() {
        RotatingTraceStoreState ts = state;
        if (ts.getCurrent() == null) open();
    }

    private void checkRotate() {
        RotatingTraceStoreState ts = state;
        if (ts.getCurrent() != null) {
            // There is current
            if (ts.getCurrent().length() > storeSize * CompositeIndex.MB) {
                synchronized (this) {
                    ts = state;
                    if (ts.getCurrent().length() > storeSize * CompositeIndex.MB) {
                        rotate();
                    }
                }
            }
        } else {
            rotate();
        }
    }


    /**
     * Archives current log and opens next one.
     */
    private synchronized void rotate() {

        RotatingTraceStoreState ts = state;

        int currentId = ts.getCurrent() != null ? (int)ts.getCurrent().getStoreId()+1 : 1;
        String dirName = String.format("%06x", currentId);

        log.info("New store " + dirName + " in " + baseDir);
        File root = new File(baseDir, dirName);
        if (!root.exists()) {
            if (!root.mkdirs()) {
                throw new ZicoException("Cannot create directory: " + root);
            }
        } // TODO else what ?

        SimpleTraceStore current = new SimpleTraceStore(root, props, indexerCache, traceTypeResolver);

        current.open();

        state = RotatingTraceStoreState.extend(state, current);
        if (ts.getCurrent() != null)
            ts.getCurrent().archive();
    }


    @Override
    public void close() throws IOException {
        log.info("Closing rotating trace store: " + baseDir);

        RotatingTraceStoreState ts = state;

        synchronized (this) {
            state = EMPTY;
        }

        if (ts.getCurrent() != null) ts.getCurrent().close();

        for (SimpleTraceStore s : ts.getArchived()) s.close();

    }


    public Map<String, TraceDataIndexer> getIndexerCache() {
        return indexerCache;
    }


    public SimpleTraceStore getCurrent() {
        return state.getCurrent();
    }
}
