package io.zorka.tdb.text.ci;

import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.util.IntegerSeqResult;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Deprecated
public class CompositeIndexSearchResult implements IntegerSeqResult {

    private List<TextIndex> indexes;
    private List<IntegerSeqResult> results = new ArrayList<>();
    private Function<TextIndex,IntegerSeqResult> searchFn;

    CompositeIndexSearchResult(CompositeIndex index, Function<TextIndex,IntegerSeqResult> searchFn) {
        this.searchFn = searchFn;
        indexes = new ArrayList<>(index.getCState().getSearchIndexes());

        for (int i = 0; i < indexes.size(); i++) {
            results.add(null);
        }
    }


    private IntegerSeqResult getSubResult(int n) {
        IntegerSeqResult rslt = results.get(n);
        if (rslt == null) {
            rslt = searchFn.apply(indexes.get(n));
            results.set(n, rslt);
        }
        return rslt;
    }

    @Override
    public int estimateSize(int sizeMax) {
        int rslt = 0;

        for (int i = 0; i < indexes.size(); i++) {
            rslt += getSubResult(i).estimateSize(sizeMax - rslt);
            if (rslt >= sizeMax) break;
        }

        return rslt;
    }

    @Override
    public int getNext() {
        while (results.size() > 0) {
            int rslt = getSubResult(0).getNext();
            if (rslt == -1) {
                indexes.remove(0);
                results.remove(0);
            } else {
                return rslt;
            }
        }
        return -1;
    }


}
