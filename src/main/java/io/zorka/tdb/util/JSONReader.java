/*
 * Copyright 2016-2019 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
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

package io.zorka.tdb.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// TODO bean serialization this only covers basic cases, so it is not complete nor secure;
// TODO implement useful set of unit tests, refactor and implement corner cases;

public class JSONReader {

    protected static final Object OBJECT_END = new Object();
    protected static final Object ARRAY_END = new Object();
    protected static final Object COLON = new Object();
    protected static final Object COMMA = new Object();
    public static final int FIRST = 0;
    public static final int CURRENT = 1;
    public static final int NEXT = 2;

    protected static Map<Character, Character> escapes = new HashMap<Character, Character>();
    static {
        escapes.put('"', '"');
        escapes.put('\\', '\\');
        escapes.put('/', '/');
        escapes.put('b', '\b');
        escapes.put('f', '\f');
        escapes.put('n', '\n');
        escapes.put('r', '\r');
        escapes.put('t', '\t');
    }

    protected CharacterIterator it;
    protected char c;
    protected Object token;
    protected StringBuffer buf = new StringBuffer();
    
    public void reset() {
    	it = null;
    	c = 0;
    	token = null;
    	buf.setLength(0);
    }

    protected char next() {
        c = it.next();
        return c;
    }

    protected void skipWhiteSpace() {
        while (Character.isWhitespace(c)) {
            next();
        }
    }

    public Object read(CharacterIterator ci, int start) {
    	reset();
        it = ci;
        switch (start) {
        case FIRST:
            c = it.first();
            break;
        case CURRENT:
            c = it.current();
            break;
        case NEXT:
            c = it.next();
            break;
        }
        return read();
    }

    public Object read(CharacterIterator it) {
        return read(it, NEXT);
    }

    public Object read(String string) {
        return read(new StringCharacterIterator(string), FIRST);
    }

    protected Object read() {
        skipWhiteSpace();
        char ch = c;
        next();
        switch (ch) {
            case '"': token = string(); break;
            case '[': token = array(); break;
            case ']': token = ARRAY_END; break;
            case ',': token = COMMA; break;
            case '{':
                token = object();
                break;
            case '}': token = OBJECT_END; break;
            case ':': token = COLON; break;
            case 't':
                next(); next(); next(); // assumed r-u-e
                token = Boolean.TRUE;
                break;
            case'f':
                next(); next(); next(); next(); // assumed a-l-s-e
                token = Boolean.FALSE;
                break;
            case 'n':
                next(); next(); next(); // assumed u-l-l
                token = null;
                break;
            default:
                c = it.previous();
                if (Character.isDigit(c) || c == '-') {
                    token = number();
                }
        }
        // System.out.println("token: " + token); // enable this line to see the token stream
        return token;
    }

    protected Object object() {
        Map<Object, Object> ret = new LinkedHashMap<Object, Object>();
        Object key = read();
        while (token != OBJECT_END) {
            read(); // should be a colon
            if (token != OBJECT_END) {
                ret.put(key, read());
                if (read() == COMMA) {
                    key = read();
                }
            }
        }

        return ret;
    }

    protected Object array() {
        List ret = new ArrayList();
        Object value = read();
        while (token != ARRAY_END) {
            ret.add(value);
            if (read() == COMMA) {
                value = read();
            }
        }
        return ret;
    }

    protected Object number() {
        int length = 0;
        boolean isFloatingPoint = false;
        buf.setLength(0);

        if (c == '-') {
            add();
        }
        length += addDigits();
        if (c == '.') {
            add();
            length += addDigits();
            isFloatingPoint = true;
        }
        if (c == 'e' || c == 'E') {
            add();
            if (c == '+' || c == '-') {
                add();
            }
            addDigits();
            isFloatingPoint = true;
        }
 
        String s = buf.toString();
        return isFloatingPoint 
            ? (length < 17) ? (Object)Double.valueOf(s) : new BigDecimal(s)
            : (length < 19) ? (Object)Long.valueOf(s) : new BigInteger(s);
    }
 
    protected int addDigits() {
        int ret;
        for (ret = 0; Character.isDigit(c); ++ret) {
            add();
        }
        return ret;
    }

    protected Object string() {
        buf.setLength(0);
        while (c != '"') {
            if (c == '\\') {
                next();
                if (c == 'u') {
                    add(unicode());
                } else {
                    Object value = escapes.get(Character.valueOf(c));
                    if (value != null) {
                        add(((Character) value).charValue());
                    }
                }
            } else {
                add();
            }
        }
        next();

        return buf.toString();
    }

    protected void add(char cc) {
        buf.append(cc);
        next();
    }

    protected void add() {
        add(c);
    }

    protected char unicode() {
        int value = 0;
        for (int i = 0; i < 4; ++i) {
            switch (next()) {
            case '0': case '1': case '2': case '3': case '4': 
            case '5': case '6': case '7': case '8': case '9':
                value = (value << 4) + c - '0';
                break;
            case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
                value = (value << 4) + (c - 'a') + 10;
                break;
            case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
                value = (value << 4) + (c - 'A') + 10;
                break;
            }
        }
        return (char) value;
    }
}