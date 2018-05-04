package io.zorka.tdb.test.unit.text.ci;

import io.zorka.tdb.text.AbstractTextIndex;
import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.text.re.SearchPatternNode;
import io.zorka.tdb.util.IntegerSeqResult;
import io.zorka.tdb.util.ZicoUtil;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class TextIndexR extends AbstractTextIndex implements TextIndex {

    protected final NavigableMap<Integer,String> words = new TreeMap<>();
    protected int idBase, nWords;
    protected long dataLen;

    public TextIndexR(int idBase, int nWords, long dataLen) {
        this.idBase = idBase;
        this.nWords = nWords;
        this.dataLen = dataLen;
    }

    @Override
    public String getPath() {
        return String.format("test-%08x.ifm", idBase);
    }

    @Override
    public int getIdBase() {
        return idBase;
    }

    @Override
    public int getNWords() {
        synchronized (words) {
            return nWords >= 0 ? nWords : words.size();
        }
    }

    @Override
    public long getDatalen() {
        return dataLen;
    }

    @Override
    public byte[] get(int id) {
        synchronized (words) {
            String s = words.get(id);
            return s != null ? s.getBytes() : null;
        }
    }

    @Override
    public int get(byte[] buf, int offs, int len, boolean esc) {
        String s = new String(buf, offs, len, Charset.forName("utf8"));
        synchronized (words) {
            for (Map.Entry<Integer, String> e : words.entrySet()) {
                if (s.equals(e.getValue())) {
                    return e.getKey();
                }
            }
        }
        return -1;
    }

    @Override
    public long length() {
        return dataLen;
    }

    @Override
    public IntegerSeqResult searchIds(SearchPatternNode node) {
        return null;
    }

    @Override
    public IntegerSeqResult searchIds(SearchPattern pattern) {
        return null;
    }

    @Override
    public IntegerSeqResult searchXIB(byte[] phrase, byte m1) {
        return null;
    }

    @Override
    public void close() { }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TextIndexR) {
            TextIndexR x = (TextIndexR)obj;
            return idBase == x.idBase
                    && nWords == x.nWords && dataLen == x.dataLen
                    && ZicoUtil.objEquals(words, x.words);
        }
        return false;
    }

    @Override
    public String toString() {
        return "R(" + idBase + "," + nWords + "," + dataLen + ")";
    }

    public NavigableMap<Integer,String> getWords() {
        return words;
    }
}
