package io.zorka.tdb.text.fm;

import io.zorka.tdb.meta.MetaIndexUtils;
import io.zorka.tdb.search.rslt.SearchResult;
import io.zorka.tdb.util.ZicoUtil;

import static io.zorka.tdb.meta.MetadataTextIndex.FIDS_MARKER;
import static io.zorka.tdb.meta.MetadataTextIndex.TIDS_MARKER;
import static io.zorka.tdb.meta.MetadataTextIndex.TID_MARKER;

public class FmTextSearchIdsResult implements SearchResult {

    private FmTextIndex index;
    private int sptr, eptr;

    public FmTextSearchIdsResult(FmTextIndex index, int tid, boolean deep) {
        this.index = index;

        initSearch(tid, deep);

    }

    private void initSearch(int tid, boolean deep) {
        byte[] buf = MetaIndexUtils.encodeMetaInt(TID_MARKER, tid, deep ? FIDS_MARKER : TIDS_MARKER);
        ZicoUtil.reverse(buf);
        long range = index.locateL(buf);
        if (range != -1L) {
            sptr = index.sp(range);
            eptr = index.ep(range);
        } else {
            sptr = eptr = -1;
        }
    }

    @Override
    public long nextResult() {
        while (sptr >= 0 && sptr <= eptr) {
            int pos = sptr++;
            int id = index.extractId(pos);
            if (id >= 0) return id;
        }
        return -1;
    }

    @Override
    public int estimateSize(int limit) {
        int rslt = eptr - sptr;
        return rslt >= 0 ? rslt + 1 : 0;
    }
}
