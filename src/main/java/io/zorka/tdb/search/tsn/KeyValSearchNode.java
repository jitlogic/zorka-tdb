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

package io.zorka.tdb.search.tsn;

import io.zorka.tdb.search.ssn.StringSearchNode;

/**
 * Searches trace data for key-value pairs
 */
public class KeyValSearchNode implements TraceSearchNode {

    private String key;
    private StringSearchNode val;

    public KeyValSearchNode(String key, StringSearchNode val) {
        this.key = key;
        this.val = val;
    }

    public String getKey() {
        return key;
    }

    public StringSearchNode getVal() {
        return val;
    }
}
