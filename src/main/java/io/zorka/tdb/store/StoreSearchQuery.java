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

package io.zorka.tdb.store;

import io.zorka.tdb.meta.MetadataSearchQuery;
import io.zorka.tdb.text.re.SearchPattern;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class StoreSearchQuery extends MetadataSearchQuery {

    /** Various search phrases for full-text search */
    private List<String> patterns = new ArrayList<>();

    /** Search flags */
    private int sflags;

    /** End index (for rotating store) */
    private int lidx;

    private int spos;

    private int limit;

    public List<String> getPatterns() {
        return patterns;
    }

    public void addPattern(String pattern) {
        patterns.add(pattern);
    }

    public int getSflags() {
        return sflags;
    }

    public void setSflags(int sflags) {
        this.sflags = sflags;
    }

    public boolean hasSFlag(int flag) {
        return 0 != (sflags & flag);
    }

    public int getLidx() {
        return lidx;
    }

    public void setLidx(int lidx) {
        this.lidx = lidx;
    }

    public int getSPos() {
        return spos;
    }

    public void setSPos(int spos) {
        this.spos = spos;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }
}
