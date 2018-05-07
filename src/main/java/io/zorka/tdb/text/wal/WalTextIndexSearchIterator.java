package io.zorka.tdb.text.wal;

import io.zorka.tdb.text.RawDictCodec;
import io.zorka.tdb.text.re.SearchBufView;
import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.text.re.SearchPatternNode;
import io.zorka.tdb.util.IntegerGetter;
import io.zorka.tdb.text.RawDictCodec;
import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.text.re.SearchPatternNode;

import java.nio.ByteBuffer;

@Deprecated
public class WalTextIndexSearchIterator implements IntegerGetter, SearchBufView {

    private WalTextIndex index;
    private Integer rslt = null;
    private int pos;
    private SearchPatternNode node;
    private boolean bstart;
    private ByteBuffer buf;
    private byte[] idbuf = new byte[6];

    WalTextIndexSearchIterator(WalTextIndex index, SearchPatternNode node) {
        this.index = index;
        this.node = node;
        this.pos = index.getFilePos() - 1;
        this.buf = index.getMappedBuffer();
        bstart = SearchPattern.nodeEndsWith(this.node, RawDictCodec.MARK_ID1);
        moveNext();
    }

    private void moveUntil(byte b) {
        while (pos > 3 && buf.get(pos) != b) pos--;
    }

    private void moveNext() {
        rslt = null;
        do {
            moveUntil(RawDictCodec.MARK_TXT); pos--;
            if (pos > 4 && node.match(this) >= 0) {
                if (bstart) {
                    pos++;
                } else {
                    moveUntil(RawDictCodec.MARK_ID1);
                }
                int pos1 = pos;
                moveUntil(RawDictCodec.MARK_ID2);
                int len = pos1 - pos - 1;
                for (int i = 0; i < len; i++) {
                    idbuf[i] = buf.get(pos + i + 1);
                }
                rslt = (int)RawDictCodec.idDecode(idbuf, 0, len);

            }
        } while (pos > 4 && rslt == null);
    }

    @Override
    public int get() {
        int i = this.rslt != null ? this.rslt : -1;
        moveNext();
        return i;
    }

    @Override
    public int position() {
        return pos;
    }

    @Override
    public void position(int pos) {
        this.pos = pos;
    }

    @Override
    public int nextChar() {
        return pos > 4 ? buf.get(pos--) : -1;
    }

    @Override
    public boolean drained() {
        int i = this.rslt != null ? this.rslt : -1;
        return i <= RawDictCodec.MARK_LAST;
    }

}
