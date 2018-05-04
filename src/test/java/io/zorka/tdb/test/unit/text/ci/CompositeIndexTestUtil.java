package io.zorka.tdb.test.unit.text.ci;

import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.text.ci.CompositeIndexStore;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

public class CompositeIndexTestUtil {
    private static final Pattern RE_IDX = Pattern.compile("^([RW])(\\d+)(,\\d+)?(,\\d+)?$");

    public static TextIndex i(String s) {
        Matcher m = RE_IDX.matcher(s);
        assertTrue("Bad index: '" + s + "'", m.matches());
        int idBase = Integer.parseInt(m.group(2));
        int nWords = m.group(3) != null ? Integer.parseInt(m.group(3).substring(1)) : 10;
        int dataLen = m.group(4) != null ? Integer.parseInt(m.group(4).substring(1)) : 1024;
        return "R".equals(m.group(1))
                ? new TextIndexR(idBase, nWords, dataLen)
                : new TextIndexW(idBase, nWords, dataLen);
    }


    public static List<TextIndex> I(String...ss) {
        List<TextIndex> rslt = new ArrayList<>(ss.length);
        for (String s : ss) {
            TextIndex idx = i(s);
            rslt.add(idx);
        }
        return rslt;
    }


    public static CompositeIndexStore BS(String...tdefs) {
        return new TestCompositeIndexStore(I(tdefs));
    }


}
