package io.zorka.tdb.test.unit.text.fm;

import io.zorka.tdb.text.fm.BWT;
import org.junit.Test;

import java.nio.charset.Charset;

import static org.junit.Assert.*;

public class BWTEncodingUnitTest {

    @Test
    public void testEncodeDecodeBwtString() {
        String s0 = "- And who is Zed ?\n- Zed's dead baby, Zed's dead.";
        byte[] bwt = new byte[s0.getBytes().length];
        int pidx = BWT.bwtencode(s0.getBytes(), bwt);
        byte[] txt = BWT.bwtdecode(bwt, pidx);
        String s1 = new String(txt, Charset.forName("utf8"));
        assertEquals(s0, s1);
    }
}
