package io.zorka.tdb.test.support;

import io.zorka.tdb.search.SearchNode;
import io.zorka.tdb.search.rslt.SearchResult;
import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.text.TextIndexState;
import io.zorka.tdb.text.WritableTextIndex;

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
    public SearchResult searchIds(long tid, boolean deep) {
        return index.searchIds(tid, deep);
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
    public SearchResult search(SearchNode expr) {
        return index.search(expr);
    }

    @Override
    public void close() throws IOException {
        index.close();
    }
}
