/*
 * Copyright 2016-2019 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
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

package io.zorka.tdb.test.unit.text.ci;

import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.text.WritableTextIndex;
import io.zorka.tdb.text.ci.CompositeIndexStore;
import io.zorka.tdb.text.WritableTextIndex;
import io.zorka.tdb.text.ci.CompositeIndexStore;

import java.util.List;

public class TestCompositeIndexStore implements CompositeIndexStore {

    private List<TextIndex> indexes;

    public TestCompositeIndexStore(List<TextIndex> indexes) {
        this.indexes = indexes;
    }

    @Override
    public String getPath() {
        return "/tmp";
    }

    @Override
    public List<TextIndex> listAll() {
        return indexes;
    }

    @Override
    public WritableTextIndex addIndex(int idBase) {
        return new TextIndexW(idBase, 10, 1024);
    }

    @Override
    public TextIndex mergeIndex(List<TextIndex> indexes) {
        TextIndexR x1 = (TextIndexR)(indexes.get(0)), x2 = (TextIndexR)(indexes.get(1));
        int idb = Math.min(x1.getIdBase(), x2.getIdBase());
        int nwo = idb + x1.getNWords() + x2.getNWords();
        long dtl = x2.getDatalen() + x2.getDatalen();
        TextIndexR rslt = new TextIndexR(idb, nwo, dtl);
        rslt.getWords().putAll(x1.getWords());
        rslt.getWords().putAll(x2.getWords());
        return rslt;
    }

    @Override
    public void removeIndex(TextIndex index) {
        // TODO mark removal somehow (so test can detect it)
    }

    @Override
    public TextIndex compressIndex(TextIndex index) {
        TextIndexR rslt = new TextIndexR(index.getIdBase(), index.getNWords(), index.getDatalen());
        rslt.getWords().putAll(((TextIndexR)index).getWords());
        return rslt;
    }
}
