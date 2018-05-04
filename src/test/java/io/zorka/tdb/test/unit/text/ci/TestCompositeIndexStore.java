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
