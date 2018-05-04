package io.zorka.tdb.text.fm;

import io.zorka.tdb.text.RawDictCodec;
import io.zorka.tdb.util.IntegerSeqResult;
import io.zorka.tdb.util.ZicoUtil;

import java.util.Arrays;

import static io.zorka.tdb.text.fm.FmIndexStore.chi;
import static io.zorka.tdb.text.fm.FmIndexStore.chr;
import static io.zorka.tdb.text.fm.FmIndexStore.rnk;

public class FmTextIndexSearchXIBResult implements IntegerSeqResult {

    private FmTextIndex index;

    private byte[] phrase;
    private int m1;
    private int sptr = -1, eptr = -1;

    FmTextIndexSearchXIBResult(FmTextIndex index, byte[] phrase, byte m1) {
        this.index = index;
        this.phrase = Arrays.copyOf(phrase, phrase.length);
        this.m1 = m1 & 0xff;
        ZicoUtil.reverse(this.phrase);
        long range = index.locateL(this.phrase);
        if (range != -1) {
            sptr = index.sp(range);
            eptr = index.ep(range);
        }
    }

    @Override
    public int estimateSize(int sizeMax) {
        return eptr - sptr + 1;
    }

    @Override
    public int getNext() {
        if (sptr == -1 || sptr > eptr) return -1;
        int rslt = 0, pos = sptr++;
        for (int i = 0; i < 8; i++) {
            long car = index.getFileStore().charAndRank(pos);
            int ch = chi(car);
            if (ch < RawDictCodec.ENC_OFF || ch >= RawDictCodec.ENC_OFF + 64 || ch == m1) break;
            rslt |= (ch - RawDictCodec.ENC_OFF) << (6*i);
            pos = index.getFileStore().getCharOffs(chr(car)) + rnk(car);
        }
        return rslt;
    }

}
