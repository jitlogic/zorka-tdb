package io.zorka.tdb.text.ci;


import io.zorka.tdb.text.AbstractTextIndex;
import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.text.WritableTextIndex;
import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.text.re.SearchPatternNode;
import io.zorka.tdb.util.IntegerSeqResult;
import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.text.WritableTextIndex;
import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.text.re.SearchPatternNode;
import io.zorka.tdb.util.IntegerSeqResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompositeIndex extends AbstractTextIndex implements WritableTextIndex {

    private static final Logger log = LoggerFactory.getLogger(CompositeIndex.class);

    public static final long MB = 1024 * 1024;

    public static final int DEFAULT_WAL_SIZE = 4096;

    private volatile CompositeIndexState cstate;
    private volatile boolean archived;
    private final int removalTimeout;
    private final CompositeIndexStore store;
    private final Executor executor;

    private final int maxWals, baseSize, maxSize;
    private final boolean stagedMerge;
    private final int maxGens;

    public CompositeIndex(CompositeIndexStore store, Properties props,
                          Executor executor) {

        this.store = store;
        this.archived = "true".equalsIgnoreCase(props.getProperty("archived", "false"));
        this.removalTimeout = Integer.parseInt(props.getProperty("rotation.removal_timeout", "10000"));

        this.maxWals = Integer.parseInt(props.getProperty("rotation.max_wals", "1"));
        this.baseSize = Integer.parseInt(props.getProperty("rotation.base_size", ""+DEFAULT_WAL_SIZE)) * 1024;
        this.stagedMerge = "true".equalsIgnoreCase(props.getProperty("rotation.staged_merge", "false"));
        this.maxGens = Integer.parseInt(props.getProperty("rotation.max_gens", "8"));
        this.maxSize = Integer.parseInt(props.getProperty("rotation.max_size", "262144")) * 1024;

        this.executor = executor;

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

        executor.execute(this::runMaintenance);
    }

    private final Object MAINTENANCE_LOCK = new Object();

    /**
     * This method performs all background maintenance tasks: compression, merges, removal.
     */
    public boolean runMaintenance() {

        synchronized (MAINTENANCE_LOCK) {

            int tasksDone = 0;

            boolean archived = this.archived;

            log.debug("Starting maintenance cycle ...");

            for (TextIndex idx : findCompressIndexes(getCState().getAllIndexes(), archived)) {
                try {
                    log.debug("Compressing index: " + idx);
                    changeState(store.compressIndex(idx), true);
                    tasksDone++;
                } catch (Exception e) {
                    log.error("Error compressing index " + idx, e);
                }
            }

            List<TextIndex> midx = stagedMerge
                    ? findMergeCandidate(getCState().getAllIndexes())
                    : findMergeCoalescing(getCState().getAllIndexes());

            if (midx != null) {
                try {
                    log.debug("Merging indexes (binary_merge = " + stagedMerge + "): " + midx);
                    changeState(store.mergeIndex(midx), true);
                    tasksDone++;
                } catch (Exception e) {
                    log.error("Error merging indexes " + midx, e);
                }
            }

            for (TextIndex idx : findRemoveFmIndexes(getCState().getAllIndexes())) {
                log.debug("Marking index " + idx + " for removal.");
                idx.markForRemoval(removalTimeout);
            }

            List<TextIndex> removeWals = findRemoveWalIndexes(getCState().getAllIndexes());
            if (archived || removeWals.size() > maxWals) {
                for (TextIndex idx : archived ? removeWals : removeWals.subList(0, removeWals.size() - maxWals)) {
                    log.debug("Marking index " + idx + " for removal.");
                    idx.markForRemoval(removalTimeout);
                }
            }

            for (TextIndex idx : getCState().getAllIndexes()) {
                if (idx.canRemove()) {
                    try {
                        log.debug("Removing index: " + idx);
                        changeState(idx, false);
                        store.removeIndex(idx);
                    } catch (Exception e) {
                        log.error("Error removing index " + idx);
                    }
                }
            }

            log.debug("Finishing maintenance cycle (tasksDone=" + tasksDone + ")");

            if (tasksDone > 0) {
                executor.execute(this::runMaintenance);
            }

            return tasksDone > 0;
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


    public boolean isArchived() {
        return archived;
    }

    public synchronized void archive() {

        log.info("Archiving index: " + store.getPath());

        archived = true;
        executor.execute(this::runMaintenance);
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

            WritableTextIndex compressIdx = cidx;

            // Rotation required
            int idBase = cidx.getIdBase() + cidx.getNWords();
            cidx = store.addIndex(idBase);
            changeState(cidx, true);

            id = cidx.add(buf, offs, len, esc);
            executor.execute(this::runMaintenance);
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
    public IntegerSeqResult searchIds(SearchPatternNode node) {
        return new CompositeIndexSearchResult(this, t -> t.searchIds(node));
    }


    @Override
    public IntegerSeqResult searchIds(SearchPattern pattern) {
        return searchIds(pattern.getInverted());
    }


    public IntegerSeqResult searchIds(String pattern) {
        return new CompositeIndexSearchResult(this, t -> t.searchIds(pattern));
    }


    @Override
    public IntegerSeqResult searchXIB(byte[] phrase, byte m1) {
        return new CompositeIndexSearchResult(this, t -> t.searchXIB(phrase, m1));
    }


    @Override
    public void close() throws IOException {
        for (TextIndex idx : cstate.getAllIndexes()) {
            idx.close();
        }
    }

    public static int icmp(TextIndex x1, TextIndex x2) {
        return x1.getIdBase() == x2.getIdBase() ? x2.getNWords() - x1.getNWords() : x1.getIdBase() - x2.getIdBase();
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
        List<TextIndex> wx = filterIndexes(idxs, TextIndex::isWritable, TextIndex::isOpen);
        int idbw = wx.size() > 0 ? wx.get(0).getIdBase() : Integer.MAX_VALUE;
        List<TextIndex> rx = filterIndexes(idxs, TextIndex::isReadOnly, TextIndex::isOpen, x -> x.getIdBase() < idbw);

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
        List<TextIndex> wx = filterIndexes(ax, TextIndex::isWritable, TextIndex::isOpen);
        List<TextIndex> rx = filterIndexes(ax, TextIndex::isReadOnly, TextIndex::isOpen);

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
        List<TextIndex> wx = filterIndexes(idxs, TextIndex::isWritable, TextIndex::isOpen);
        List<TextIndex> rx = filterIndexes(idxs, TextIndex::isReadOnly, TextIndex::isOpen);

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
        List<TextIndex> wx = filterIndexes(idxs, TextIndex::isWritable, TextIndex::isOpen);
        List<TextIndex> rx = filterIndexes(idxs, TextIndex::isReadOnly, TextIndex::isOpen);

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
        List<TextIndex> rx = filterIndexes(idxs, TextIndex::isReadOnly, TextIndex::isOpen);
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
        List<TextIndex> rx = filterIndexes(idxs, TextIndex::isReadOnly, TextIndex::isOpen);
        List<TextIndex> wx = filterIndexes(idxs, TextIndex::isWritable, TextIndex::isOpen);

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

    /**
     * Looks for candidate for merge in normal working condition.
     */
    public List<TextIndex> findMergeCandidate(List<TextIndex> idxs) {
        List<TextIndex> xs = filterIndexes(idxs, TextIndex::isReadOnly, TextIndex::isOpen);

        while (xs.size() > 1) {
            TextIndex x1 = xs.get(xs.size()-2), x2 = xs.get(xs.size()-1);
            int g1 = toGen(x1.getDatalen(), baseSize), g2 = toGen(x2.getDatalen(), baseSize);
            if (g1 >= maxGens || g2 >= maxGens) {
                // One of indexes has already reached maximum size
                return null;
            } else if (g1 == g2) {
                // Two smaller indexes from the same generation are suitable for merge
                return Arrays.asList(x1, x2);
            } else if (g1 > g2) {
                // Last index awaits for pair from its generation
                xs.remove(xs.size()-1);
            } else {
                // This should not happen except for rare circumstances (bugs etc.).
                // TODO log warning here
                return Arrays.asList(x1, x2);
            }
        }

        return null;
    }


    public List<TextIndex> findMergeCoalescing(List<TextIndex> idxs) {
        List<TextIndex> xs = filterIndexes(idxs,
                TextIndex::isReadOnly, TextIndex::isOpen,
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

}
