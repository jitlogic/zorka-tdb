package io.zorka.tdb.text.fm;

import io.zorka.tdb.text.RawDictCodec;
import io.zorka.tdb.text.re.SearchBufView;
import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.text.re.SearchPatternNode;
import io.zorka.tdb.text.re.SeqPatternNode;
import io.zorka.tdb.util.IntegerGetter;
import io.zorka.tdb.text.RawDictCodec;
import io.zorka.tdb.text.re.SearchBufView;
import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.text.re.SearchPatternNode;
import io.zorka.tdb.text.re.SeqPatternNode;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static io.zorka.tdb.text.fm.FmIndexStore.chr;
import static io.zorka.tdb.text.fm.FmIndexStore.rnk;

@Deprecated
public class FmTextIndexRegexIterator implements IntegerGetter, SearchBufView {
    private FmTextIndex index;
    private Set<Integer> rslts = new HashSet<>();
    private Integer rslt = null;
    private byte[] buf = new byte[512];
    private int bpos = 0, bsplit = 0, blen = 0;
    private boolean bstart = false, bstop = false;
    private byte[] idbuf = new byte[9];
    private int sptr = -1, eptr = -2;
    private SearchPatternNode node;


    public FmTextIndexRegexIterator(FmTextIndex index, SearchPatternNode node) {
        this.index = index;
        this.node = node;
        this.node.visitMtns(this::handleNode);
        this.bstart = SearchPattern.nodeEndsWith(this.node, RawDictCodec.MARK_ID1);
        this.bstop = SearchPattern.nodeStartsWith(this.node, RawDictCodec.MARK_TXT);
        moveNext();
    }


    private void handleNode(SearchPatternNode node) {
        if (sptr != -2) {
            long range = index.locateL(((SeqPatternNode)node).getText());

            if (range == -1) {
                sptr = -2;
            } else {
                int sp = index.sp(range), ep = index.ep(range);
                if (sptr == -1 || ep-sp < eptr-sptr) {
                    sptr = sp;
                    eptr = ep;
                }
            }
        }
    }


    private int extract(int pos0) {
        int pos = pos0;

        bpos = bsplit = blen = 0;

        if (!bstart) {

            // Extract first half of term
            for (int i = 0; i < FmTextIndex.CHUNK_MAX; i++) {
                long car = index.getFileStore().charAndRank(pos);
                byte ch = FmIndexStore.chr(car);
                if (ch == RawDictCodec.MARK_ID1) break;
                addByte(ch);
                pos = index.getFileStore().getCharOffs(ch) + FmIndexStore.rnk(car);
            }

            bsplit = blen;

            // Extract ID
            pos = index.skip(pos, 1);
        }

        int id = index.extractId(pos);

        // Shortcut for repeated ID (already matched once)
        if (rslts.contains(id)) {
            return id;
        }

        // Find end of string
        long range = index.locateById(id);
        if (range == -1 || index.sp(range) != index.ep(range)) {
            return -1;  // Should not happen, possibly malformed index
        }

        pos = index.sp(range);

        if (bstart) {
            addByte(RawDictCodec.MARK_ID1);
        }

        bpos = bsplit = blen;

        if (bstop) {
            addByte(RawDictCodec.MARK_TXT);
        }

        // Extract second half of term
        for (int i = 0; i < FmTextIndex.CHUNK_MAX && pos != pos0; i++) {
            long car = index.getFileStore().charAndRank(pos);
            byte ch = FmIndexStore.chr(car);
            addByte(ch);
            if (bstart && ch == RawDictCodec.MARK_ID1) break;
            if (ch <= RawDictCodec.MARK_ID2) return -1;
            pos = index.getFileStore().getCharOffs(ch) + FmIndexStore.rnk(car);
        }

        return id;
    }

    private void addByte(byte ch) {
        if (blen >= buf.length) {
            buf = Arrays.copyOf(buf, buf.length + 512);
        }
        buf[blen++] = ch;
    }

    private void moveNext() {
        rslt = null;
        while (sptr >= 0 && sptr <= eptr) {
            int id = extract(sptr++);
            if (id != -1 && !rslts.contains(id) && node.match(this) > 0) {
                rslts.add(rslt);
                rslt = id;
                break;
            }
        }
    }


    @Override
    public int get() {
        int i = rslt != null ? rslt : -1;
        moveNext();
        return i;
    }

    @Override
    public int position() {
        return bpos;
    }

    @Override
    public void position(int pos) {
        this.bpos = pos;
    }

    @Override
    public int nextChar() {
        if (bpos != -1) {
            int rslt = buf[bpos++] & 0xff;
            if (bpos >= blen) {
                // TODO lazy buffered read of characters from FM index
                // Two variants are possible:
                // 1) eagerly read first part (up to bsplit), then lazy read second part
                // 2) lazy read both parts and use transformed regex to compensate for
                bpos = 0;
            } else if (bpos == bsplit) {
                bpos = -1;
            }
            return rslt;

        } else {
            return -1;
        }
    }

    @Override
    public boolean drained() {
        int i = rslt != null ? rslt : -1;
        return i <= RawDictCodec.MARK_LAST;
    }
}
