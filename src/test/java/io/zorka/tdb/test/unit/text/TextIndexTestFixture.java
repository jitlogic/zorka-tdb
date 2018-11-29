package io.zorka.tdb.test.unit.text;

import io.zorka.tdb.test.support.TestUtil;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.text.TextIndexType;
import io.zorka.tdb.text.WalTextIndex;
import io.zorka.tdb.text.WritableTextIndex;
import org.junit.After;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;

import static org.junit.Assert.fail;

public class TextIndexTestFixture extends ZicoTestFixture {

    protected final TextIndexType type;
    protected TextIndex idx = null;

    @Parameterized.Parameters(name="type={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {TextIndexType.WAL},
                {TextIndexType.FMI}
        });
    }

    @After
    public void shutDown() throws Exception {
        if (idx != null) {
            idx.close();
            idx = null;
        }
    }

    public TextIndexTestFixture(TextIndexType type) {
        this.type = type;
    }

    protected void newIndex(Consumer<WritableTextIndex> c) throws Exception {

        shutDown();

        WalTextIndex wal = new WalTextIndex(TestUtil.path(tmpDir, "idx1.wal"), 0, 64 * 1024, 4);

        c.accept(wal);

        switch (type) {
            case WAL:
                idx = wal;
                break;
            case FMI:
                idx = toFmIndex(wal);
                wal.close();
                break;
            default:
                fail("Not supported index type: " + type);
        }

    }

}
