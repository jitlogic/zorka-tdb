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

package io.zorka.tdb.test.support;

import java.util.Arrays;
import java.util.List;

public class ListBasedStrGen implements TestStrGen {

    private int idx;
    private List<String> values;

    public ListBasedStrGen(String...vals) {
        values = Arrays.asList(vals);
    }

    @Override
    public String get() {
        return idx < values.size() ? values.get(idx++) : null;
    }

    @Override
    public void reset() {
        idx = 0;
    }
}
