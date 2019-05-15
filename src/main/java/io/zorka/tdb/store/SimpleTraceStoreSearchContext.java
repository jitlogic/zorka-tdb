package io.zorka.tdb.store;

import io.zorka.tdb.util.BitmapSet;
import org.mapdb.Fun;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

import static io.zorka.tdb.store.SimpleTraceStore.ERROR_BIT;

public class SimpleTraceStoreSearchContext {

    private int skip;
    private int count;
    private int limit;

    private TraceSearchQuery query;
    private SimpleTraceStore store;

    private ConcurrentNavigableMap<Long,Long> tstamps;
    private Long tstampC;

    private List<ConcurrentNavigableMap<Fun.Tuple3<Integer,Integer,Long>,Long>> sattrs = new ArrayList<>();
    private List<Fun.Tuple3<Integer,Integer,Long>> sattrC = new ArrayList<>();

    private BitmapSet stringIds;
    private ConcurrentNavigableMap<Fun.Tuple2<Integer,Long>,Long> strings;

    public SimpleTraceStoreSearchContext(SimpleTraceStore store, TraceSearchQuery query, int limit, int offset) {
        this.query = query;
        this.store = store;
        this.tstamps = store.getTstamps();
        this.skip = offset;
        this.count = 0;
        this.limit = limit;
        this.strings = store.getStrings();
    }

    private boolean init() {
        this.tstamps = store.getTstamps();

        if (query.getMinTstamp() != -1 || query.getMaxTstamp() != -1) {
            tstamps = tstamps.subMap(query.getMinTstamp(), query.getMaxTstamp());
        }

        if (tstamps.isEmpty()) return false;

        this.tstampC = tstamps.lastKey();

        for (Map.Entry<String,String> am : query.getAttrMatches().entrySet()) {
            int k = store.getTextIndex().get(am.getKey());
            int v = store.getTextIndex().get(am.getValue());
            if (k <= 0 || v <= 0) return false;
            ConcurrentNavigableMap<Fun.Tuple3<Integer,Integer,Long>,Long> m = store.getSattrs().subMap(
                    Fun.t3(k,v,0L), Fun.t3(k, v, Long.MAX_VALUE));
            if (m.isEmpty()) return false;
            sattrs.add(m);
            sattrC.add(m.lastKey());
        }

        if (query.getText() != null) {
            stringIds = new BitmapSet();
            store.getTextIndex().search(query.getText(), query.hasMatchStart(), query.hasMatchEnd(), stringIds);
        }

        return true;
    }

    /**
     * Returns lowest timestamp for all index cursors.
     */
    private Long lowestTstamp() {
        if (tstampC == null) return null;

        long tst = tstampC;

        for (int i = 0; i < sattrC.size(); i++) {
            if (sattrC.get(i) == null) return null;
            tst = Math.min(tst, sattrC.get(i).c);
        }

        return tst;
    }

    private boolean align() {
        long md = query.getMinDuration();
        boolean ef = query.hasErrorsOnly();
        boolean cde = md != 0 || ef;

        for (Long tst = lowestTstamp(); tst != null; tst = lowestTstamp()) {
            if (cde) {
                long dur = tstamps.get(tst);
                if ((dur & ~ERROR_BIT) < md) {
                    tstampC = tstamps.lowerKey(tst);
                    continue;
                }
                if (ef && 0 == (dur & ERROR_BIT)) {
                    tstampC = tstamps.lowerKey(tst);
                    continue;
                }
            }

            boolean match = true;

            for (int i = 0; i < sattrs.size(); i++) {
                Fun.Tuple3<Integer,Integer,Long> c = sattrC.get(i);
                Fun.Tuple3<Integer,Integer,Long> c2 = sattrs.get(i).floorKey(Fun.t3(c.a,c.b,tst));
                if (c2 == null) return false;
                if (!tst.equals(c2.c)) { match = false; break; }
                sattrC.set(i,c2);
            }

            if (match) {
                boolean smatch = stringIds == null;
                if (!smatch) {
                    for (int id = stringIds.first(); id > 0; id = stringIds.next(id)) {
                        if (strings.containsKey(Fun.t2(id, tst))) {
                            smatch = true;
                            break;
                        }
                    }
                }
                if (smatch) {
                    tstampC = tst;
                    return true;
                }
            }
            tstampC = tstamps.lowerKey(tst);
        }

        return false;
    }

    public int search(List<ChunkMetadata> acc) {

        if (!init()) return 0;

        while (tstampC != null && acc.size() < limit) {
            if (!align()) break;

            if (skip == 0) {
                acc.add(store.getChunkMetadata(tstampC));
            } else {
                skip--;
            }

            tstampC = tstamps.lowerKey(tstampC);
            count++;
        }

        return count;
    }

}
