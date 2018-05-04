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

/**
 *
 */
public class ClassPatternNode implements SearchPatternNode {

    private final static long L1 = 0xffffffffffffffffL;

    private boolean invert;
    private long[] chmap;

    public static final String DIGITS = "0123456789";
    public static final String SPACES = " \t\n\r";
    public static final String WORDS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";

    public static ClassPatternNode empty() {
        return new ClassPatternNode(0, 0, 0, 0);
    }

    public final static int MIN_CHAR = 4;


    public static ClassPatternNode all() {
        return new ClassPatternNode(L1, L1, L1, L1);
    }

    public static ClassPatternNode ctlChars(boolean invert) {
        ClassPatternNode node = empty();
        for (int i = 0; i < 32; i++) {
            node.setChar(i);
        }
        node.setInvert(invert);
        return node;
    }


    public static ClassPatternNode encNums(boolean invert) {
        ClassPatternNode node = empty();
        for (int i = '0'; i < '0'+64; i++) {
            node.setChar(i);
        }
        node.setInvert(invert);
        return node;
    }


    public static ClassPatternNode def(String chars, boolean invert) {
        ClassPatternNode node = empty();
        for (int i = 0; i < chars.length(); i++) {
            node.setChar(chars.charAt(i));
        }
        node.setInvert(invert);
        return node;
    }


    private ClassPatternNode(long c1, long c2, long c3, long c4) {
        chmap = new long[] { c1, c2, c3, c4 };
    }


    public boolean isEmpty() {
        return chmap[0] == 0 && chmap[1] == 0 && chmap[2] == 0 && chmap[3] == 0;
    }


    public boolean matches(int c) {
        int i = c >>> 6;
        long v = 1L << (c & 63);
        return (0 != (chmap[i] & v)) ^ invert;
    }


    void setChar(int c) {
        int i = c >>> 6;
        long v = 1L << (c & 63);
        chmap[i] |= v;
    }


    void setChar(int c1, int c2) {
        for (int c = c1; c <= c2; c++) {
            setChar(c);
        }
    }


    void setInvert(boolean invert) {
        this.invert = invert;
    }


    @Override
    public String toString() {
        if (chmap[0] == L1 && chmap[1] == L1 && chmap[2] == L1 && chmap[3] == L1) {
            return ".";
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            if (invert)
                sb.append("^");
            for (int i = 0; i < 256; i++) {
                if (matches(i) ^ invert) {
                    sb.append((char)i);
                }
            }
            sb.append("]");
            return sb.toString();
        }
    }

    @Override
    public SearchPatternNode invert() {
        return this;
    }

    @Override
    public int match(SearchBufView view) {
        int pos = view.position();
        int c = view.nextChar();
        if (c < MIN_CHAR || !matches(c)) {
            view.position(pos);
            return ZERO_FAIL;
        }
        return 1;
    }

    @Override
    public void visitMtns(SearchPatternNodeHandler handler) {
    }
}
