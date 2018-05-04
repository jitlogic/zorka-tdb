/*
 * Copyright 2016-2017 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
 * <p/>
 * This is free software. You can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * <p/>
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * <p/>
 * You should have received a copy of the GNU General Public License
 * along with this software. If not, see <http://www.gnu.org/licenses/>.
 */

package io.zorka.tdb.text.re;

import io.zorka.tdb.ZicoException;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.util.BitSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Somewhat similar to regular expression, yet not complete and specialized for searching over
 * structured / metadata indexes.
 */
public class SearchPattern {

    public enum Token {


        GR_BEGIN  (false),
        GR_END    (false),
        CL_BEGIN  (false),
        CL_END    (false),
        QT_BEGIN  (true),
        QT_END    (true),

        DOT       (false),
        STAR      (true),
        PLUS      (true),
        MINUS     (false),
        QMARK     (true),
        DASH      (false),
        DOLLAR    (false),
        PIPE      (false),

        DIGIT     (false),
        NON_DIGIT (false),
        SPACE     (false),
        NON_SPACE (false),
        WORD      (false),
        NON_WORD  (false),
        CTRL      (false),
        NON_CTRL  (false),
        BNUM      (false),
        NON_BNUM  (false);

        public final boolean isQualifier;

        Token(boolean isQualifier) {
            this.isQualifier = isQualifier;
        }
    }


    private String regex;
    private SearchPatternNode root, inverted;
    private PushbackReader reader;
    private Object lastToken = null;
    private int ngroups = 0;


    public static int h2i(int h) {
        if (h == -1) {
            throw new ZicoException("Unexpected end of input.");
        }
        if (h >= '0' && h <= '9') {
            return h - '0';
        }
        if (h >= 'a' && h <= 'f') {
            return h - 'a' + 10;
        }
        if (h >= 'A' && h <= 'F') {
            return h - 'A' + 10;
        }
        throw new ZicoException("Expected hexadecimal number but got '" + ((char)h) + "'");
    }


    private Object read(boolean clmode) {
        try {
            int ch = reader.read();
            switch (ch) {
                case -1: return null;
                case '(': return clmode ? ch : Token.GR_BEGIN;
                case ')': return clmode ? ch : Token.GR_END;
                case '[': return clmode ? ch : Token.CL_BEGIN;
                case ']': return Token.CL_END;
                case '{': return clmode ? ch : Token.QT_BEGIN;
                case '}': return clmode ? ch : Token.QT_END;
                case '.': return Token.DOT;
                case '*': return Token.STAR;
                case '+': return Token.PLUS;
                case '?': return Token.QMARK;
                case '^': return Token.DASH;
                case '$': return clmode ? ch : Token.DOLLAR;
                case '|': return clmode ? ch : Token.PIPE;
                case '-': return clmode ? Token.MINUS : ch;
                case '\\': {
                    int c0 = reader.read();
                    switch (c0) {
                        case 'd': return Token.DIGIT;
                        case 'D': return Token.NON_DIGIT;
                        case 's': return Token.SPACE;
                        case 'S': return Token.NON_SPACE;
                        case 'w': return Token.WORD;
                        case 'W': return Token.NON_WORD;
                        case 'h': return Token.CTRL;
                        case 'H': return Token.NON_CTRL;
                        case 'b': return Token.BNUM;
                        case 'B': return Token.NON_BNUM;
                        case 'n': return '\n';
                        case 't': return '\t';
                        case 'r': return '\r';
                        case 'x': return 16 * h2i(reader.read()) + h2i(reader.read());
                        default:  return c0;
                    }
                }
                default:
                    return ch;
            }
        } catch (IOException e) {
            throw new ZicoException("I/O error while reading regexp (should not happen).", e);
        }
    }


    public static char i2c(Object obj) {
        if (obj instanceof Number) {
            return (char)((Number) obj).intValue();
        }
        throw new ZicoException("Illegal object type for object: " + obj);
    }


    private Object readToken(boolean clmode) {
        Object token = lastToken != null ? lastToken : read(clmode);
        lastToken = null;

        if (token == null) return null;

        if (token instanceof Integer) {
            StringBuilder sb = new StringBuilder();
            sb.append(i2c(token));
            while ((token = read(clmode)) instanceof Integer) {
                sb.append(i2c(token));
            }
            lastToken = token;
            return sb.toString();
        }
        return token;
    }


    private SearchPatternNode conj(SearchPatternNode rslt, SearchPatternNode node) {
        return rslt != null ? new ConjPatternNode(rslt, node) : node;
    }


    private SearchPatternNode loop(SearchPatternNode node, int min, int max) {
        if (node == null) {
            throw new ZicoException("Encountered qualifier but expected some regex first.");
        }
        if (node instanceof SeqPatternNode && ((SeqPatternNode)node).getText().length > 1) {
            byte[] buf = ((SeqPatternNode) node).getText();
            return new ConjPatternNode(
                new SeqPatternNode(buf, 0, buf.length - 1),
                new LoopPatternNode(new SeqPatternNode(buf, buf.length - 1, 1), min, max));
        } else if (node instanceof ConjPatternNode) {
            ConjPatternNode cn = (ConjPatternNode)node;
            return new ConjPatternNode(
                cn.getNode1(),
                new LoopPatternNode(cn.getNode2(), min, max));
        } else {
            return new LoopPatternNode(node, min, max);
        }
    }


    private SearchPatternNode parseClass() {
        Object token, prev = null;
        ClassPatternNode rslt = ClassPatternNode.empty();

        while (Token.CL_END != (token = read(true))) {
            if (token == null) {
                throw new ZicoException("Unexpected end of regular expression.");
            }

            if (token == Token.DASH) {
                rslt.setInvert(true);
            } else if (token == Token.MINUS) {
                Object t1 = read(true);
                if (prev instanceof Integer && t1 instanceof Integer) {
                    rslt.setChar((Integer)prev, (Integer)t1);
                    prev = null;
                } else {
                    throw new ZicoException("Syntax error when defining character span.");
                }
            } else if (token instanceof Integer) {
                rslt.setChar(i2c(token));
            } else {
                throw new ZicoException("Illegal char.");
            }

            prev = token;
        }

        return rslt;
    }


    private final static Pattern RE_INT1 = Pattern.compile("^(\\d+)$");
    private final static Pattern RE_INT2 = Pattern.compile("^(\\d+),(\\d+)$");


    private SearchPatternNode parseLoop(SearchPatternNode rslt) {
        Object t1 = readToken(false), t2 = readToken(false);
        if (t1 instanceof String && t2 == Token.QT_END) {
            Matcher m1 = RE_INT1.matcher((String)t1);
            if (m1.matches()) {
                int n = Integer.parseInt(m1.group(1));
                return loop(rslt, n, n);
            }
            Matcher m2 = RE_INT2.matcher((String)t1);
            if (m2.matches()) {
                int n1 = Integer.parseInt(m2.group(1)), n2 = Integer.parseInt(m2.group(2));
                if (n1 > n2) {
                    throw new ZicoException("Invalid qualifier range: ");
                }
                return loop(rslt, n1, n2);
            }
        }

        throw new ZicoException("Syntax error: invalid qualifier.");
    }


    private SearchPatternNode parse(Object until) {
        Object token;
        SearchPatternNode rslt = null;

        while (until != (token = readToken(false))) {
            if (token == null) {
                throw new ZicoException("Unexpected end of regular expression.");
            } else if (token.getClass() == String.class) {
                rslt = conj(rslt, new SeqPatternNode((String)token));
            } else if (token.getClass() == Token.class) {
                switch ((Token)token) {
                    case CL_BEGIN:
                        rslt = conj(rslt, parseClass());
                        continue;
                    case GR_BEGIN:
                        ngroups++;
                        rslt = conj(rslt, new GroupPatternNode(parse(Token.GR_END),""+ngroups));
                        continue;
                    case QT_BEGIN:
                        rslt = parseLoop(rslt);
                        continue;
                    case DOT:
                        rslt = conj(rslt, ClassPatternNode.all());
                        continue;
                    case PIPE:
                        rslt = new AltPatternNode(rslt, parse(until));
                        continue;
                    case PLUS:
                        rslt = loop(rslt, 1, Integer.MAX_VALUE);
                        continue;
                    case STAR:
                        rslt = loop(rslt, 0, Integer.MAX_VALUE);
                        continue;
                    case QMARK:
                        rslt = loop(rslt, 0, 1);
                        continue;
                    case DIGIT:
                        rslt = conj(rslt, ClassPatternNode.def(ClassPatternNode.DIGITS, false));
                        continue;
                    case NON_DIGIT:
                        rslt = conj(rslt, ClassPatternNode.def(ClassPatternNode.DIGITS, true));
                        continue;
                    case SPACE:
                        rslt = conj(rslt, ClassPatternNode.def(ClassPatternNode.SPACES, false));
                        continue;
                    case NON_SPACE:
                        rslt = conj(rslt, ClassPatternNode.def(ClassPatternNode.SPACES, true));
                        break;
                    case WORD:
                        rslt = conj(rslt, ClassPatternNode.def(ClassPatternNode.WORDS, false));
                        break;
                    case NON_WORD:
                        rslt = conj(rslt, ClassPatternNode.def(ClassPatternNode.WORDS, true));
                        break;
                    case CTRL:
                        rslt = conj(rslt, ClassPatternNode.ctlChars(false));
                        continue;
                    case NON_CTRL:
                        rslt = conj(rslt, ClassPatternNode.ctlChars(true));
                        continue;
                    case BNUM:
                        rslt = conj(rslt, ClassPatternNode.encNums(false));
                        continue;
                    case NON_BNUM:
                        rslt = conj(rslt, ClassPatternNode.encNums(true));
                        continue;
                    default:
                        throw new ZicoException("Unexpected token: " + token);
                }
            }
        }

        return rslt;
    }

    private static final BitSet CTL_CHARS = new BitSet(256);

    static {
        for (byte b : "()[]{}.*+?^$|-\\".getBytes()) {
            CTL_CHARS.set(b & 0xff);
        }
    }

    public static String escape(byte[] buf) {
        return escape(buf, 0, buf.length);
    }

    public static String escape(byte[] buf, int offs, int len) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < len; i++) {
            byte b = buf[i+offs];
            if (CTL_CHARS.get(b)) {
                sb.append('\\');
                if (b == (byte)'\\') {
                    b = buf[++i];
                }
            }
            sb.append((char)b);
        }

        return sb.toString();
    }

    public static SearchPattern search(String text) {
        return new SearchPattern(new SeqPatternNode(text));
    }

    public static SearchPattern search(String text, boolean matchStart, boolean matchEnd) {
        SeqPatternNode node = new SeqPatternNode(text);
        node.setMatchStart(matchStart);
        node.setMatchEnd(matchEnd);
        return new SearchPattern(node);
    }

    public static SearchPattern search(byte[] b, boolean matchStart, boolean matchEnd) {
        SeqPatternNode node = new SeqPatternNode(b);
        node.setMatchStart(matchStart);
        node.setMatchEnd(matchEnd);
        return new SearchPattern(node);
    }

    public static SearchPattern search(byte[] b) {
        return new SearchPattern(new SeqPatternNode(b));
    }

    public final static SearchPattern NO_MATCH = new SearchPattern(new NoMatchPatternNode());

    public SearchPattern(SearchPatternNode root) {
        this.root = root;
        this.inverted = root.invert();
    }


    public SearchPattern(String regex) {
        this.regex = regex;
        this.reader = new PushbackReader(new StringReader(regex));
        this.root = parse(null);
        this.inverted = this.root.invert();
        this.reader = null;
    }


    public SearchPatternNode getRoot() {
        return root;
    }

    public SearchPatternNode getInverted() { return inverted; }

    public static boolean nodeStartsWith(SearchPatternNode n, byte b) {
        while (n instanceof ConjPatternNode) {
            n = ((ConjPatternNode)n).getNode1();
        }
        if (n instanceof SeqPatternNode) {
            byte[] text = ((SeqPatternNode)n).getText();
            return b == text[0];
        }
        return false;
    }

    public static boolean nodeEndsWith(SearchPatternNode n, byte b) {
        while (n instanceof ConjPatternNode) {
            n = ((ConjPatternNode)n).getNode2();
        }
        if (n instanceof SeqPatternNode) {
            byte[] text = ((SeqPatternNode)n).getText();
            return b == text[text.length-1];
        }
        return false;
    }

}
