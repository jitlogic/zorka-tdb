/*
 * Copyright 2016-2017 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
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

package io.zorka.tdb.text.fm;

import io.zorka.tdb.search.rslt.SearchResult;
import io.zorka.tdb.search.ssn.TextNode;
import io.zorka.tdb.text.RawDictCodec;
import io.zorka.tdb.util.ZicoUtil;

import java.util.HashSet;
import java.util.Set;

public class FmTextSearchResult implements SearchResult {

    private FmTextIndex index;
    private TextNode node;
    private int sptr, eptr;
    private Set<Integer> ids = new HashSet<>();


    public FmTextSearchResult(FmTextIndex index, TextNode node) {
        this.index = index;
        this.node = node;

        initSearch();
    }


    private void initSearch() {
        byte[] buf = new byte[node.getText().length + (node.isMatchStart() ? 1 : 0) + (node.isMatchEnd() ? 1 : 0)];
        System.arraycopy(node.getText(), 0, buf, node.isMatchStart() ? 1 : 0, node.getText().length);

        if (node.isMatchStart()) {
            buf[0] = RawDictCodec.MARK_TXT;
        }

        if (node.isMatchEnd()) {
            buf[buf.length-1] = RawDictCodec.MARK_ID2;
        }

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
        boolean bskip = !node.isMatchStart();
        while (sptr >= 0 && sptr <= eptr) {
            int pos = bskip ? index.skipUntil(sptr++, RawDictCodec.MARK_ID1, true) : sptr++;
            if (pos >= 0) {
                if (bskip) pos = index.skip(pos, 1);
                int id = index.extractId(pos);
                if (id >= 0 && !ids.contains(id)) {
                    ids.add(id);
                    return id;
                }
            }

        }
        return -1;
    }


    @Override
    public int estimateSize(int limit) {
        int rslt = eptr - sptr;
        return rslt >= 0 ? rslt + 1 : 0;
    }

}
