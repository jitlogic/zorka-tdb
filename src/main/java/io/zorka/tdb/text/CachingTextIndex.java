package io.zorka.tdb.text;

import io.zorka.tdb.util.BitmapSet;

import java.io.IOException;
import java.util.Arrays;

public class CachingTextIndex extends AbstractTextIndex implements WritableTextIndex {

    private final StringCache cache;
    private WritableTextIndex idx;

    public CachingTextIndex(WritableTextIndex idx, int cacheSize) {
        this.idx = idx;
        this.cache = new StringCache(cacheSize);
    }

    public WritableTextIndex getParentIndex() {
        return idx;
    }

    @Override
    public String getPath() {
        return idx.getPath();
    }

    @Override
    public int getIdBase() {
        return idx.getIdBase();
    }

    @Override
    public int getNWords() {
        return idx.getNWords();
    }

    @Override
    public long getDatalen() {
        return idx.getDatalen();
    }

    @Override
    public byte[] get(int id) {
        return idx.get(id);
    }

    @Override
    public int get(String s) {
        int id;

        id = cache.get(s);

        if (id >= 0) return id;
        id = idx.get(s);
        if (id < 0) return id;

        cache.add(id, s);

        return id;
    }

    @Override
    public int get(byte[] buf) {
        String s = new String(buf);

        int id;

        id = cache.get(s);

        if (id >= 0) return id;
        id = idx.get(buf);
        if (id < 0) return id;

        cache.add(id, s);

        return id;
    }

    @Override
    public int get(byte[] buf, int offs, int len, boolean esc) {
        String s = new String(Arrays.copyOfRange(buf, offs, offs+len));
        int id;

        id = cache.get(s);

        if (id >= 0) return id;
        id = idx.get(buf, offs, len, esc);
        if (id < 0) return id;

        cache.add(id, s);

        return id;
    }

    @Override
    public long length() {
        return idx.length();
    }


    @Override
    public void close() throws IOException {
        idx.close();
    }

    @Override
    public int add(String s) {
        int id;

        id = cache.get(s);

        if (id >= 0) return id;
        id = idx.add(s);
        if (id < 0) return id;

        cache.add(id, s);

        return id;
    }

    @Override
    public int add(byte[] buf) {
        String s = new String(buf);

        int id;

        id = cache.get(s);

        if (id >= 0) return id;
        id = idx.add(buf);
        if (id < 0) return id;

        cache.add(id, s);

        return id;
    }

    @Override
    public int add(byte[] buf, int offs, int len, boolean esc) {
        String s = new String(Arrays.copyOfRange(buf, offs, offs+len));
        int id;

        id = cache.get(s);

        if (id >= 0) return id;
        id = idx.add(buf, offs, len, esc);
        if (id < 0) return id;

        cache.add(id, s);

        return id;
    }

    @Override
    public void flush() {
        idx.flush();
    }

    @Override
    public int search(String text, boolean matchStart, boolean matchEnd, BitmapSet rslt) {
        return idx.search(text, matchStart, matchEnd, rslt);
    }
}
