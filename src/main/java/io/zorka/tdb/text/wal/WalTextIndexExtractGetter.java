package io.zorka.tdb.text.wal;

import io.zorka.tdb.text.RawDictCodec;
import io.zorka.tdb.util.IntegerGetter;
import io.zorka.tdb.util.ZicoUtil;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class WalTextIndexExtractGetter implements IntegerGetter {
    private WalTextIndex index;
    private byte[] phrase;
    private byte m1, plast;
    private int pos, plen;
    private ByteBuffer buf;

    WalTextIndexExtractGetter(WalTextIndex index, byte[] phrase, byte m1) {
        this.plen = phrase.length;
        this.phrase = Arrays.copyOf(phrase, phrase.length);
        ZicoUtil.reverse(this.phrase);
        this.plast = this.phrase[0];
        this.m1 = m1;
        this.pos = index.getFilePos() - 1;
        this.buf = index.getMappedBuffer();
    }

    private void moveUntil(byte b) {
        while (pos > 3 && buf.get(pos) != b) pos--;
    }

    private boolean find(byte c) {
        while (pos > 3) {
            byte b = buf.get(pos);
            if (b == c) return true;
            if (b >= 0 && b == RawDictCodec.MARK_TXT) return false;
            pos--;
        }
        return false;
    }

    private boolean matches() {
        if (pos < phrase.length+4) return false;
        for (int i = 1; i < phrase.length; i++) {
            if (buf.get(pos-i) !=  phrase[i]) return false;
        }
        return true;
    }

    private int extract() {
        int rslt = 0;
        for (int i = 0; i < 8; i++) {
            int v = buf.get(pos-i) & 0xff;
            if (v == m1 || v < RawDictCodec.ENC_OFF) {
                pos -= i;
                break;
            }
            rslt |= (v - RawDictCodec.ENC_OFF) << (6 * i);
        }

        return rslt;
    }

    @Override
    public int get() {
        do {
            moveUntil(RawDictCodec.MARK_TXT); pos--;
            while (find(plast)) {
                if (matches()) {
                    pos -= plen;
                    return extract();
                } else {
                    pos--;
                }
            }
        } while (pos > 4);
        return -1;
    }
}
