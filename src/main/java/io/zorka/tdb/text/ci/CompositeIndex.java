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

package io.zorka.tdb.text.ci;


import io.zorka.tdb.text.AbstractTextIndex;
import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.search.SearchNode;
import io.zorka.tdb.text.WritableTextIndex;
import io.zorka.tdb.util.BitmapSet;
import io.zorka.tdb.util.ZicoMaintObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class CompositeIndex extends AbstractTextIndex implements WritableTextIndex, ZicoMaintObject {

    private static final Logger log = LoggerFactory.getLogger(CompositeIndex.class);

    public static final long MB = 1048576;
    public static final int DEFAULT_WAL_SIZE = 4096;
    public static final int MAINT_CYCLES_MAX = 3;

    private final Lock mlock = new ReentrantLock();

    private volatile CompositeIndexState cstate;
    private volatile boolean archived;
    private final CompositeIndexStore store;

    private final int baseSize;
    private final int maxSize;

    public CompositeIndex(CompositeIndexStore store, Properties props) {

        this.store = store;
        this.archived = "true".equalsIgnoreCase(props.getProperty("archived", "false"));

        this.baseSize = Integer.parseInt(props.getProperty("rotation.base_size", ""+DEFAULT_WAL_SIZE)) * 1024;
        this.maxSize = Integer.parseInt(props.getProperty("rotation.max_size", "262144")) * 1024;

        open();
    }


    private synchronized void open() {
        log.debug("Opening index: " + store.getPath());
        List<TextIndex> allIndexes = store.listAll();
        List<TextIndex> searchIndexes = findSearchIndexes(allIndexes);
        List<TextIndex> lookupIndexes = archived ? findLookupIndexesArchived(allIndexes) : findLookupIndexes(allIndexes);

        if ((lookupIndexes.size() == 0 || !lookupIndexes.get(0).isWritable()) && !archived) {
            int idBase = 1;
            for (TextIndex idx : lookupIndexes) {
                idBase = Math.max(idBase, idx.getIdBase() + idx.getNWords());
            }
            TextIndex wx = store.addIndex(idBase);
            allIndexes.add(wx);
            lookupIndexes.add(0, wx);
            searchIndexes.add(0, wx);
        }

        cstate = new CompositeIndexState(allIndexes, lookupIndexes, searchIndexes, archived);

    }

    /**
     * This method performs all background maintenance tasks: compression, merges, removal.
     */
    @Override
    public boolean runMaintenance() {

        if (!mlock.tryLock()) return false;

        try {
            int tasksDone;
            log.debug("Starting maintenance cycle ...");

            runRemovalCycle();
            tasksDone = runCompressCycle(archived);
            tasksDone += runMergeCycle();
            runRemovalCycle();

            log.debug("Finishing maintenance cycle (tasksDone=" + tasksDone + ")");

            return tasksDone > 0;
        } finally {
            mlock.unlock();
        }
    }


    private int runMergeCycle() {
        int tasksDone = 0;
        List<TextIndex> midx = findMergeCoalescing(getCState().getAllIndexes());

        if (midx != null) {
            try {
                log.debug("Merging indexes: " + midx);
                changeState(store.mergeIndex(midx), true);
                tasksDone++;
            } catch (Exception e) {
                log.error("Error merging indexes " + midx, e);
            }
        }
        return tasksDone;
    }


    private int runCompressCycle(boolean archived) {
        int tasksDone = 0;
        for (TextIndex idx : findCompressIndexes(getCState().getAllIndexes(), archived)) {
            try {
                log.debug("Compressing index: " + idx);
                changeState(store.compressIndex(idx), true);
                tasksDone++;
            } catch (Exception e) {
                log.error("Error compressing index " + idx, e);
            }
        }
        return tasksDone;
    }


    private void runRemovalCycle() {
        List<TextIndex> idxs = getCState().getAllIndexes();

        for (TextIndex idx : findRemoveFmIndexes(idxs)) {
            removeIndex(idx);
        }

        List<TextIndex> removeWals = findRemoveWalIndexes(idxs);
        for (TextIndex idx : removeWals) {
            removeIndex(idx);
        }
    }


    private void removeIndex(TextIndex idx) {
        try {
            log.debug("Removing index file: " + idx);
            changeState(idx, false);
            idx.close();
            store.removeIndex(idx);
        } catch (Exception e) {
            log.error("Cannot remove index " + idx + ". Will try again.", e);
        }
    }


    private void changeState(TextIndex index, boolean addIndex) {
        synchronized (this) {
            List<TextIndex> allIndexes = new ArrayList<>(cstate.getAllIndexes());
            if (addIndex) {
                allIndexes.add(index);
            } else {
                allIndexes.remove(index);
            }
            List<TextIndex> lookupIndexes = archived ? findLookupIndexesArchived(allIndexes) : findLookupIndexes(allIndexes);
            List<TextIndex> searchIndexes = findSearchIndexes(allIndexes);
            cstate = new CompositeIndexState(allIndexes, lookupIndexes, searchIndexes, archived);
        }
    }


    public CompositeIndexState getCState() {
        return cstate;
    }


    public synchronized void archive() {
        log.info("Archiving index: " + store.getPath());
        archived = true;
    }


    @Override
    public int add(byte[] buf, int offs, int len, boolean esc) {
        if (len == 0) {
            log.warn("Tried to add empty string.", new RuntimeException());
            return 0;
        }

        int id;

        if ((id = get(buf, offs, len, esc)) >= 0) {
            return id;
        }

        WritableTextIndex cidx = cstate.getCurrentIndex();
        if (cidx == null) return -1;

        // Optimistic variant - just adding
        id = cidx.add(buf, offs, len, esc);
        if (id != -1) return id;

        synchronized (this) {
            cidx = cstate.getCurrentIndex();

            // Check again (if state has changed)
            id = cidx.add(buf, offs, len, esc);
            if (id != -1) return id;

            cidx.flush();

            // Rotation required
            int idBase = cidx.getIdBase() + cidx.getNWords();
            cidx = store.addIndex(idBase);
            changeState(cidx, true);

            id = cidx.add(buf, offs, len, esc);
        }

        return id;
    }


    @Override
    public void flush() {
        cstate.getCurrentIndex().flush();
    }


    @Override
    public String getPath() {
        return store.getPath();
    }


    @Override
    public int getIdBase() {
        int idBase = Integer.MAX_VALUE;
        for (TextIndex idx : cstate.getLookupIndexes()) {
            idBase = Math.min(idBase, idx.getIdBase());
        }
        return idBase;
    }


    @Override
    public int getNWords() {
        int nWords = 0;
        for (TextIndex idx : cstate.getLookupIndexes()) {
            nWords += idx.getNWords();
        }
        return nWords;
    }


    @Override
    public long getDatalen() {
        long dataLen = 0;
        for (TextIndex idx : cstate.getLookupIndexes()) {
            dataLen += idx.getDatalen();
        }
        return dataLen;
    }


    @Override
    public byte[] get(int id) {
        for (TextIndex idx : cstate.getLookupIndexes()) {
            if (idx.isOpen()) {
                byte[] rslt = idx.get(id);
                if (rslt != null) {
                    return rslt;
                }
            }
        }
        return null;
    }


    @Override
    public int get(byte[] buf, int offs, int len, boolean esc) {
        for (TextIndex idx : cstate.getLookupIndexes()) {
            if (idx.isOpen()) {
                int rslt = idx.get(buf, offs, len, esc);
                if (rslt >= 0) return rslt;
            }
        }
        return -1;
    }


    @Override
    public long length() {
        long len = 0;
        for (TextIndex idx : cstate.getSearchIndexes()) {
            len += idx.length();
        }
        return len;
    }


    @Override
    public void close() throws IOException {
        for (TextIndex idx : cstate.getAllIndexes()) {
            idx.close();
        }
    }


    private static int icmp(TextIndex x1, TextIndex x2) {
        return x1.getIdBase() == x2.getIdBase() ? x2.getNWords() - x1.getNWords() : x1.getIdBase() - x2.getIdBase();
    }


    private static boolean isIndexOpen(TextIndex idx) {
        return idx != null && idx.isOpen();
    }


    /**
     * Filters out overlapping indexes (unnecessary for searches or lookups).
     */
    static void filterOverlaps(List<TextIndex> rx) {
        rx.sort(CompositeIndex::icmp);
        for (int i = 0; i < rx.size()-1; i++) {
            TextIndex x = rx.get(i);
            while (rx.size()-1 > i) {
                TextIndex y = rx.get(i+1);
                if (x.getFirstId() <= y.getFirstId() && x.getLastId() >= y.getLastId()) {
                    rx.remove(i+1);
                } else {
                    break;
                }
            }
        }
    }


    /**
     *
     */
    public List<TextIndex> filterIndexes(List<TextIndex> indexes, Predicate<? super TextIndex>...predicates) {
        Stream<TextIndex> stream = indexes.stream();

        for (Predicate<? super TextIndex> p : predicates) {
            stream = stream.filter(p);
        }

        return stream.sorted(CompositeIndex::icmp).collect(Collectors.toList());
    }

    /**
     * Finds all indexes suitable for fast lookup operations and constructs properly ordered list of them.
     * WAL indexes are preferred as their lookup operations implementation is fast.
     *
     * @param idxs list of all indexes (both WAL and FM); function assumes that WAL indexes cover contiguous
     *             ID range and no FM index ends past this range;
     *
     * @return list ordered in the following way: WAL indexes in reversed ID range order, then remaining indexes
     *         in normal order, excluding ones fully overlapping with either WAL or other FM indexes;
     */
    public List<TextIndex> findLookupIndexes(List<TextIndex> idxs) {
        List<TextIndex> wx = filterIndexes(idxs, TextIndex::isWritable, CompositeIndex::isIndexOpen);
        int idbw = wx.size() > 0 ? wx.get(0).getIdBase() : Integer.MAX_VALUE;
        List<TextIndex> rx = filterIndexes(idxs, TextIndex::isReadOnly, CompositeIndex::isIndexOpen, x -> x.getIdBase() < idbw);

        Collections.reverse(wx);
        filterOverlaps(rx);

        if (rx.size() > 1) {
            Collections.reverse(rx);
            rx.add(0, rx.get(rx.size()-1));
            rx.remove(rx.size()-1);
        }

        List<TextIndex> rslt = new ArrayList<>(idxs.size());
        rslt.addAll(wx); rslt.addAll(rx);
        return rslt;
    }


    public List<TextIndex> findLookupIndexesArchived(List<TextIndex> ax) {
        List<TextIndex> wx = filterIndexes(ax, TextIndex::isWritable, CompositeIndex::isIndexOpen);
        List<TextIndex> rx = filterIndexes(ax, TextIndex::isReadOnly, CompositeIndex::isIndexOpen);

        if (rx.size() == 0) return wx;

        TextIndex r = rx.get(rx.size()-1);

        ArrayList<TextIndex> rslt = new ArrayList<>();

        for (TextIndex w : wx) {
            if (r.getLastId() <= w.getFirstId()) rslt.add(w);
        }

        rslt.addAll(rx);
        rslt.sort(CompositeIndex::icmp);

        return rslt;
    }


    /**
     * Finds all indexes suitable for general search operations and constructs properly ordered list of them.
     * FMI indexes are preferred to WAL indexes with exception of most recent WAL index if it is still writable.
     */
    public List<TextIndex> findSearchIndexes(List<TextIndex> idxs) {
        List<TextIndex> wx = filterIndexes(idxs, TextIndex::isWritable, CompositeIndex::isIndexOpen);
        List<TextIndex> rx = filterIndexes(idxs, TextIndex::isReadOnly, CompositeIndex::isIndexOpen);

        filterOverlaps(rx);
        Collections.reverse(rx);
        Collections.reverse(wx);

        List<TextIndex> rslt = new ArrayList<>(idxs.size());
        for (TextIndex w : wx) {
            boolean overlap = false;
            for (TextIndex r : rx) {
                if (w.getIdBase() >= r.getIdBase() && w.getLastId() <= r.getLastId()) {
                    overlap = true;
                    break;
                }
            }
            if (!overlap) {
                rslt.add(w);
            }
        }

        rslt.addAll(rx);

        return rslt;
    }


    /**
     * Finds all WAL indexes subject to compression. This covers only freshly rotated WAL indexes compressed for the
     * first time.
     */
    public List<TextIndex> findCompressIndexes(List<TextIndex> idxs, boolean archived) {
        List<TextIndex> wx = filterIndexes(idxs, TextIndex::isWritable, CompositeIndex::isIndexOpen);
        List<TextIndex> rx = filterIndexes(idxs, TextIndex::isReadOnly, CompositeIndex::isIndexOpen);

        if (!archived && wx.size() > 0) {
            wx.remove(wx.size()-1);
        }

        if (rx.size() == 0) return wx;

        List<TextIndex> rslt = new ArrayList<>();

        for (TextIndex x : wx) {
            boolean matches = false;
            for (TextIndex y : rx) {
                if (x.getFirstId() >= y.getFirstId() && x.getLastId() <= y.getLastId()) {
                    matches = true;
                    break;
                }
            }
            if (!matches) rslt.add(x);
        }

        return rslt;
    }


    /**
     * Locates all archived index files suitable for removal.
     */
    public List<TextIndex> findRemoveFmIndexes(List<TextIndex> idxs) {
        List<TextIndex> rx = filterIndexes(idxs, TextIndex::isReadOnly, CompositeIndex::isIndexOpen);
        List<TextIndex> rslt = new ArrayList<>();

        for (int i = 0; i < rx.size()-1; i++) {
            TextIndex x = rx.get(i);
            while (rx.size()-1 > i) {
                TextIndex y = rx.get(i+1);
                if (x.getFirstId() <= y.getFirstId() && x.getLastId() >= y.getLastId()) {
                    rx.remove(i+1);
                    rslt.add(y);
                } else {
                    break;
                }
            }
        }

        return rslt;
    }


    public List<TextIndex> findRemoveWalIndexes(List<TextIndex> idxs) {
        List<TextIndex> rx = filterIndexes(idxs, TextIndex::isReadOnly, CompositeIndex::isIndexOpen);
        List<TextIndex> wx = filterIndexes(idxs, TextIndex::isWritable, CompositeIndex::isIndexOpen);

        List<TextIndex> rslt = new ArrayList<>();

        for (TextIndex w : wx) {
            for (TextIndex r : rx) {
                if (w.getFirstId() >= r.getFirstId() && w.getLastId() <= r.getLastId()) {
                    rslt.add(w);
                    break;
                }
            }
        }

        return rslt;
    }


    public int toGen(long size, int baseSize) {
        long sz = size / baseSize;

        for (int i = 0; i < 32; i++) {
            if (sz == 0) return i;
            sz >>= 1;
        }

        return -1;
    }


    public List<TextIndex> findMergeCoalescing(List<TextIndex> idxs) {
        List<TextIndex> xs = filterIndexes(idxs,
                TextIndex::isReadOnly, CompositeIndex::isIndexOpen,
                x -> x.getDatalen() < maxSize-baseSize);

        int size = 0;
        List<TextIndex> rslt = new ArrayList<>();
        for (TextIndex x : xs) {
            if (size + x.getDatalen() < maxSize) {
                rslt.add(x);
                size += x.getDatalen();
            } else if (rslt.size() > 1) {
                return rslt;
            } else {
                rslt.clear();
                size = 0;
            }
        }

        return rslt.size() > 1 ? rslt : null;
    }


    @Override
    public int search(SearchNode expr, BitmapSet rslt) {
        int cnt = 0;

        List<TextIndex> indexes = getCState().getSearchIndexes();

        for (TextIndex idx : indexes) {
            cnt += idx.search(expr, rslt);
        }

        return cnt;
    }


}
