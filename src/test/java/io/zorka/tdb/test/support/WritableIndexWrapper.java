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

import io.zorka.tdb.search.SearchNode;
import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.text.TextIndexState;
import io.zorka.tdb.text.WritableTextIndex;
import io.zorka.tdb.util.BitmapSet;

import java.io.IOException;

import static org.junit.Assert.fail;

public class WritableIndexWrapper implements WritableTextIndex {

    private TextIndex index;
    private boolean assertive;

    public WritableIndexWrapper(TextIndex index, boolean assertive) {
        this.index = index;
        this.assertive = assertive;
    }

    @Override
    public int add(byte[] buf, int offs, int len, boolean esc) {
        if (assertive) fail("Should not happen.");
        return 0;
    }

    @Override
    public void flush() {
        if (assertive) fail("Should not happen.");
    }

    @Override
    public String getPath() {
        return index.getPath();
    }

    @Override
    public int getIdBase() {
        return index.getIdBase();
    }

    @Override
    public int getNWords() {
        return index.getNWords();
    }

    @Override
    public long getDatalen() {
        return index.getDatalen();
    }

    @Override
    public byte[] get(int id) {
        return index.get(id);
    }

    @Override
    public int get(byte[] buf, int offs, int len, boolean esc) {
        return index.get(buf, offs, len, esc);
    }

    @Override
    public long length() {
        return index.length();
    }

    @Override
    public int searchIds(long tid, boolean deep, BitmapSet rslt) {
        return index.searchIds(tid, deep, rslt);
    }

    @Override
    public TextIndexState getState() {
        return index.getState();
    }

    @Override
    public void setState(TextIndexState state) {
        if (assertive) fail("Should not happen.");
    }

    @Override
    public boolean canRemove() {
        return index.canRemove();
    }

    @Override
    public void markForRemoval(long timeout) {
        index.markForRemoval(timeout);
    }

    @Override
    public void close() throws IOException {
        index.close();
    }

    @Override
    public int search(SearchNode expr, BitmapSet rslt) {
        return 0;
    }
}
