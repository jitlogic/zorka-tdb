package io.zorka.tdb.text.fm;

import io.zorka.tdb.text.RawDictCodec;
import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.text.re.SeqPatternNode;
import io.zorka.tdb.util.IntegerGetter;

import java.util.HashSet;
import java.util.Set;

@Deprecated
public class FmTextIndexSimpleIterator implements IntegerGetter {
    private FmTextIndex index;
    private Set<Integer> rslts = new HashSet<>();
    private Integer rslt = null;
    private SeqPatternNode node;
    private boolean bskip;
    private int sptr, eptr;


    FmTextIndexSimpleIterator(FmTextIndex index, SeqPatternNode node) {
        this.index = index;
        this.node = node;
        this.sptr = 0;
        this.eptr =  index.getFileStore().getDatalen();
        this.bskip = !SearchPattern.nodeEndsWith(node, RawDictCodec.MARK_ID1);
        initSearch();
    }


    private void initSearch() {
        long range = index.locateL(node.getText());
        if (range != -1) {
            sptr = index.sp(range);
            eptr = index.ep(range);
            moveNext();
        }
    }


    private void moveNext() {
        rslt = null;
        while (rslt == null && sptr <= eptr) {
            int pos = bskip ? index.skipUntil(sptr++, RawDictCodec.MARK_ID1, true) : sptr++;
            if (pos >= 0) {
                if (bskip) pos = index.skip(pos, 1);
                int id = index.extractId(pos);
                if (id >= 0 && !rslts.contains(id)) {
                    rslts.add(id);
                    rslt = id;
                    return;
                }
            }
        }
    }

    @Override
    public int get() {
        int i = rslt != null ? rslt : -1;
        moveNext();
        return i;
    }
}
