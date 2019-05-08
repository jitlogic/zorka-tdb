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
import io.zorka.tdb.text.ci.CompositeIndex;
import com.jitlogic.zorka.common.util.ZorkaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.zorka.tdb.store.ConfigProps.*;
import static io.zorka.tdb.store.RotatingTraceStoreState.*;

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

    public RotatingTraceStore(File baseDir, Properties props, Map<String,TraceDataIndexer> indexerCache) {
        this.baseDir = baseDir;

        this.props = props;


        this.storeSize = Integer.parseInt(props.getProperty(STORES_MAX_SIZE, "16"));    // default 16GB
        this.storesMax = Integer.parseInt(props.getProperty(STORES_MAX_NUM, "16"));     // default 256GB

        this.indexerCache = indexerCache;

        if (!baseDir.exists() || !baseDir.isDirectory()) {
            throw new ZicoException("Path " + baseDir + " does not exist or is not a directory.");
        }
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

    private static final Pattern RE_SDIR = Pattern.compile("[0-9a-fA-F]{6}");

    public synchronized void reset() throws IOException {
        if (state.isOpen()) close();

        String[] lst = baseDir.list();

        if (lst != null) {
            for (String s : lst) {
                if (RE_SDIR.matcher(s).matches()) {
                    ZorkaUtil.rmrf(new File(baseDir, s));
                }
            }
        }
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
            stores.add(new SimpleTraceStore(af, props, indexerCache));
        }

        RotatingTraceStoreState ts = RotatingTraceStoreState.init(stores);
        ts.getCurrent().open();

        synchronized (this) {
            this.state = ts;
        }
    }


    public byte[] retrieveRaw(Tid t) {
        List<ChunkMetadata> chunks = getChunks(t);
        if (chunks.isEmpty()) return null;

        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        for (ChunkMetadata c : chunks) {
            c.getStore().retrieveRaw(c.getTstamp(), bos);
        }

        return bos.toByteArray();
    }


    public <T> T retrieve(Tid t, TraceDataRetriever<T> rtr) {
        List<ChunkMetadata> chunks = getChunks(t);

        // TODO check if chunk list is complete

        for (int i = 0; i < chunks.size(); i++) {
            ChunkMetadata c = chunks.get(i);
            c.getStore().retrieveChunk(c.getTstamp(), i == 0, rtr);
        }

        rtr.commit();
        return rtr.getResult();
    }


    public List<ChunkMetadata> getChunks(Tid t) {
        checkOpen();

        RotatingTraceStoreState st = state;

        List<ChunkMetadata> chunks = new ArrayList<>();
        int count = st.getCurrent().getChunks(t,chunks);

        for (int i = state.getArchived().size()-1; i >= 0; i--) {
            int cnt = state.getArchived().get(i).getChunks(t,chunks);
            if (count != 0 && cnt == 0) break;
            count += cnt;
        }

        chunks.sort(Comparator.comparingInt(ChunkMetadata::getChunkNum));

        return chunks;
    }


    public ChunkMetadata getTrace(Tid t, boolean fetchAttrs) {
        List<ChunkMetadata> chunks = getChunks(t);

        if (chunks.isEmpty()) return null;

        Map<Long,List<ChunkMetadata>> m = new TreeMap<>();

        long root = 0L;

        for (ChunkMetadata md : chunks) {
            long sid = md.getSpanId();
            if (root == 0L && md.getParentId() == 0) root = md.getSpanId();
            if (!m.containsKey(sid)) m.put(sid, new ArrayList<>());
            m.get(sid).add(md);
        }

        // Pass 1: sort chunks and set chunks field
        for (Map.Entry<Long,List<ChunkMetadata>> e : m.entrySet()) {
            List<ChunkMetadata> l = e.getValue();
            l.sort(Comparator.comparingInt(ChunkMetadata::getChunkNum));
            if (l.size() > 1) l.get(0).setChunks(l);
            l.get(0).setChildren(new ArrayList<>());
            if (fetchAttrs) {
                Map<String,Object> attrs = new TreeMap<>();
                for (ChunkMetadata c : l) c.getStore().getAttributes(c, attrs);
                l.get(0).setAttributes(attrs);
            }
        }

        // Pass 2: merge all chunk sets into a tree
        for (Map.Entry<Long,List<ChunkMetadata>> e : m.entrySet()) {
            ChunkMetadata c = e.getValue().get(0);
            long pid = c.getParentId();
            if (pid != 0) {
                List<ChunkMetadata> pl = m.get(pid);
                if (pl != null) {
                    pl.get(0).getChildren().add(c);
                }
            }
        }

        return m.get(root).get(0);
    }


    public Map<String,Object> getAttributes(Tid t) {

        Map<String,Object> rslt = new TreeMap<>();

        for (ChunkMetadata cm : getChunks(t)) {
            cm.getStore().getAttributes(cm, rslt);
        }

        return rslt;
    }


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
    public synchronized void rotate() {

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

        SimpleTraceStore current = new SimpleTraceStore(root, props, indexerCache);

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

    public SimpleTraceStore getCurrent() {
        return state.getCurrent();
    }

    public List<ChunkMetadata> searchChunks(TraceSearchQuery query, int limit, int offset) {
        checkOpen();

        List<ChunkMetadata> acc = new ArrayList<>(limit);

        RotatingTraceStoreState state = this.state;

        int count = state.getCurrent().search(query, limit, offset, acc);

        if (state.getArchived() != null) {
            for (int i = state.getArchived().size() - 1; i >= 0 && acc.size() < limit; i--) {
                SimpleTraceStore store = state.getArchived().get(i);
                int lim = limit - acc.size();
                int off = offset - count;
                count += store.search(query, lim, off, acc);
            }
        }

        if (query.hasFetchAttrs()) {
            for (ChunkMetadata c : acc) {
                Map<String,Object> attrs = new TreeMap<>();
                c.getStore().getAttributes(c, attrs);
                c.setAttributes(attrs);
            }
        }

        return acc;
    }

    public Collection<ChunkMetadata> search(TraceSearchQuery query, int limit, int offset) {
        Map<Tid,ChunkMetadata> rslt = new HashMap<>();

        int lim = limit + offset, offs = 0;

        while (rslt.size() < limit) {
            Collection<ChunkMetadata> chunks = searchChunks(query, lim, offs);
            if (chunks.size() == 0) break;

            for (ChunkMetadata c : chunks) {
                Tid tid = query.hasSpansOnly()
                        ? Tid.s(c.getTraceId1(), c.getTraceId2(), c.getSpanId())
                        : Tid.t(c.getTraceId1(), c.getTraceId2());
                if (rslt.containsKey(tid)) continue;
                rslt.put(tid, getTrace(tid, query.hasFetchAttrs()));
                if (rslt.size() >= limit) break;
            }

            if (rslt.size() < limit) {
                offs += lim;
                lim = limit;
            }
        }

        return rslt.values();
    }

}
