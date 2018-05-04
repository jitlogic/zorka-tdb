package io.zorka.tdb.test.unit.search;

import io.zorka.tdb.store.StoreSearchExprBuilder;
import io.zorka.tdb.store.StoreSearchExprParser;

import org.junit.Test;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

public class StoreSearchExprParserUnitTest {

    private static Object l(Object...objs) {
        return Arrays.asList(objs);
    }

    static class TestBuilder implements StoreSearchExprBuilder {

        @Override
        public Object stringToken(String s, boolean exact) {
            return l("s", s, exact);
        }

        @Override
        public Object regexToken(Object s) {
            return l("r", s);
        }

        @Override
        public Object functionToken(String fn, List<Object> args) {
            return l("f", fn, args);
        }

    }

    private Object p(String expr) {
        StoreSearchExprParser parser = new StoreSearchExprParser(expr, new TestBuilder());
        return parser.parse();
    }

    private Object s(String s) {
        return l("s", s, false);
    }

    private Object S(String s) {
        return l("s", s, true);
    }

    @Test
    public void testParseSingleStringRegexExprs() {
        assertEquals(s("abc"), p("abc"));
        assertEquals(l("s", "abc def", false), p("abc def"));
        assertEquals(l("s", "abc (def)", false), p("abc (def)"));
        assertEquals(l("s", "abc", false), p("'abc'"));
        assertEquals(l("s", "abc", true), p("\"abc\""));
        assertEquals(l("r", "abc"), p("~abc"));

        // TODO przetestować błędy składniowe i przypadki graniczne
    }

    @Test
    public void testParseExprs() {
        assertEquals(l("f", "==", l(s("URI"), s("/index.html"))), p("(== 'URI' '/index.html')"));
        assertEquals(l("f", "==", l(s("URI"), s("/index.html"))), p("URI == /index.html"));
        assertEquals(l("f", "==", l(s("URI"), s("/index.html"))), p("URI == '/index.html'"));
        assertEquals(l("f", "==", l(s("URI"), S("/index.html"))), p("URI == \"/index.html\""));
    }
}
