package io.zorka.tdb.text.re;

public class NoMatchPatternNode implements SearchPatternNode {
    @Override
    public SearchPatternNode invert() {
        return this;
    }

    @Override
    public int match(SearchBufView view) {
        return -1;
    }

    @Override
    public void visitMtns(SearchPatternNodeHandler handler) {
    }
}
