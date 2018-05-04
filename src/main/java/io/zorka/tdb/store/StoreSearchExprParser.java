package io.zorka.tdb.store;

import io.zorka.tdb.ZicoException;
import io.zorka.tdb.text.re.SearchPattern;
import io.zorka.tdb.text.re.SearchPattern;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses store search expressions.
 */
public class StoreSearchExprParser {

    private StoreSearchExprBuilder builder;
    private String expr;
    private PushbackReader reader;
    private Object lastToken = null;
    private int pos = 0;

    public enum Token {
        S_QUOTE,
        D_QUOTE,
        TILDE,
        X_BEGIN,
        X_END
    }

    public StoreSearchExprParser(String expr, StoreSearchExprBuilder builder) {
        this.expr = expr;
        this.builder = builder;
        this.reader = new PushbackReader(new StringReader(expr.trim()));

    }

    /**
     * Reads a token from input stream.
     * @param inq iside quote if true, else false
     * @param tch terminating character
     * @return
     */
    private Object read(boolean inq, int tch) {
        try {
            int ch = reader.read(); pos++;
            switch (ch) {
                case -1: return null;
                case '\'': return inq && ch != tch ? ch : Token.S_QUOTE;
                case '"': return inq && ch != tch ? ch : Token.D_QUOTE;
                case '~': return inq && ch != tch ? ch : Token.TILDE;
                case '(': return inq && ch != tch ? ch : Token.X_BEGIN;
                case ')': return inq && ch != tch ? ch : Token.X_END;
                case '\\': {
                    int c0 = reader.read();
                    switch (c0) {
                        case 'n': return '\n';
                        case 't': return '\t';
                        case 'r': return '\r';
                        case 'x': return 16 * SearchPattern.h2i(reader.read()) + SearchPattern.h2i(reader.read());
                        default: return c0;
                    }
                }
                default:
                    return ch;
            }
        } catch (IOException e) {
            throw new ZicoException("I/O error while reading expression (should not happen)", e);
        }
    }

    // tch = -2 --> symbol
    private Object readToken(boolean inq, int tch) {
        Object token = lastToken != null ? lastToken : read(inq, tch);
        lastToken = null;

        if (token == null) return null;

        if (token instanceof Integer) {
            StringBuilder sb = new StringBuilder();
            sb.append(SearchPattern.i2c(token));
            while ((token = read(true, tch == -2 ? ')' : tch)) instanceof Integer) {
                if (tch == -2 && Character.isWhitespace((Integer)token)) {
                    token = null; break;
                }
                sb.append(SearchPattern.i2c(token));
            }
            lastToken = token;
            return sb.toString();
        }

        return token;
    }

    private void skipSpace() {
        Object token = lastToken != null ? lastToken : read(false, -1);
        lastToken = null;

        if (token == null) return;

        while (token instanceof Integer) {
            if (!Character.isWhitespace((Integer)token)) break;
            token = read(false, -1);
        }

        lastToken = token;
    }

    private void error(String msg) {
        throw new ZicoException("" + msg + " at position " + pos + ": [" + expr + "]");
    }

    private static Pattern RE_KV = Pattern.compile("([a-zA-Z0-9_.\\-]+)\\s*(==|!=|~=)\\s*(.+)?\\s*");

    private Object unquoteString(String s) {
        if (s.length() == 0) {
            return builder.stringToken("", false);
        }
        if (s.startsWith("'")) {
            if (s.endsWith("'")) {
                return builder.stringToken(s.substring(1, s.length()-1), false);
            } else {
                error("Unexpected end of string");
            }
        }
        if (s.startsWith("\"")) {
            if (s.endsWith("\"")) {
                return builder.stringToken(s.substring(1, s.length()-1), true);
            } else {
                error("Unexpected end of string");
            }
        }
        return builder.stringToken(s, false);
    }

    public Object parse() {
        Object token = readToken(false, -1);
        if (token instanceof String) {
            String s = (String)token;
            Matcher m = RE_KV.matcher(s);
            if (m.matches()) {
                Object k = unquoteString(m.group(1));
                String op = m.group(2);
                Object v = unquoteString(m.group(3));
                return builder.functionToken(op, Arrays.asList(k, v));
            } else {
                return builder.stringToken(s, false);
            }
        } else if (Token.S_QUOTE.equals(token)) {
            Object obj = readToken(true, '\'');
            if (obj instanceof String && Token.S_QUOTE.equals(lastToken)) {
                lastToken = null;
                return builder.stringToken((String)obj, false);
            } else {
                error("Unterminated quote");
            }
        } else if (Token.D_QUOTE.equals(token)) {
            Object obj = readToken(true, '"');
            if (obj instanceof String && Token.D_QUOTE.equals(lastToken)) {
                lastToken = null;
                return builder.stringToken((String)obj, true);
            } else {
                error("Unterminated quote");
            }
        } else if (Token.TILDE.equals(token)) {
            Object obj = readToken(false, -1);
            if (obj != null) {
                return builder.regexToken(obj);
            } else {
                error("Expected string");
            }
        } else if (Token.X_BEGIN.equals(token)) {
            Object fn = readToken(false, -2);
            if (fn instanceof String) {
                skipSpace();
                List<Object> args = new ArrayList<>();
                while ((token = parse()) != Token.X_END) {
                    if (token == null) error("Unexpected end of expression.");
                    args.add(token);
                    skipSpace();
                }
                return builder.functionToken((String)fn, args);
            } else {
                error("Expected symbol.");
            }
        } else if (Token.X_END.equals(token)) {
            return token;
        }
        return null;
    } // parse()

} //
