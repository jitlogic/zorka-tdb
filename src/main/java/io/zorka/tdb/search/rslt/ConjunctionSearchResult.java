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

package io.zorka.tdb.search.rslt;

import io.zorka.tdb.search.EmptySearchResult;
import io.zorka.tdb.util.BitmapSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConjunctionSearchResult implements SearchResult {

    private List<SearchResult> inputs;
    private SearchResult result;

    public ConjunctionSearchResult(SearchResult sr1, SearchResult sr2) {
        inputs = Arrays.asList(sr1, sr2);
        initSearch();
    }

    public ConjunctionSearchResult(List<SearchResult> inputs) {
        this.inputs = new ArrayList<>(inputs);
        initSearch();
    }

    private void initSearch() {
        BitmapSet bmps = null;

        for (SearchResult sr : inputs) {
            BitmapSet b = new BitmapSet();
            for (long l = sr.nextResult(); l >= 0; l = sr.nextResult()) {
                b.set((int)l);
            }
            if (bmps == null) {
                bmps = b;
            } else {
                bmps = bmps.and(b);
            }
        }

        result = bmps != null ? bmps.searchAll() : EmptySearchResult.INSTANCE;
    }

    @Override
    public long nextResult() {
        return result.nextResult();
    }

    @Override
    public int estimateSize(int limit) {
        return result.estimateSize(limit);
    }
}
