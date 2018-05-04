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

import io.zorka.tdb.util.ZicoUtil;
import io.zorka.tdb.util.ZicoUtil;

import java.nio.charset.Charset;
import java.util.Arrays;

/**
 *
 */
public class SeqPatternNode implements SearchPatternNode {

    private boolean matchStart = false, matchEnd = false;
    private byte[] text;
    private int[] deltas; // Delta

    public SeqPatternNode(String text) {
        this.text = text.getBytes();
    }

    public SeqPatternNode(byte[] buf) {
        this(buf, 0, buf.length);
    }

    public SeqPatternNode(byte[] buf, int offs, int len) {
        text = new byte[len];
        System.arraycopy(buf, offs, text, 0, len);
    }

    public byte[] getText() {
        return text;
    }

    public String toString() {
        return "'" + new String(text) + "'";
    }

    public String asString() { return new String(text, Charset.forName("utf8")); }

    public byte[] asBytes() { return text; }

    public boolean isMatchStart() {
        return matchStart;
    }

    public void setMatchStart(boolean matchStart) {
        this.matchStart = matchStart;
    }

    public boolean isMatchEnd() {
        return matchEnd;
    }

    public boolean isExact() { return matchStart && matchEnd; }

    public void setMatchEnd(boolean matchEnd) {
        this.matchEnd = matchEnd;
    }

    @Override
    public SearchPatternNode invert() {
        byte[] txt1 = Arrays.copyOf(text, text.length);
        ZicoUtil.reverse(txt1, 0, txt1.length);
        return new SeqPatternNode(txt1);
    }

    @Override
    public int match(SearchBufView view) {
        // TODO quicker path for StringBufView
        if (matchStart) {
            int pos = view.position();
            for (int i = 0; i < text.length; i++) {
                int b = text[i] & 0xff;
                int c = view.nextChar();
                if (b != c) {
                    view.position(pos);
                    return i == 0 ? Integer.MIN_VALUE : -i;
                }
            }
            if (matchEnd && !view.drained()) {
                view.position(pos);
                return -1;
            } else {
                return text.length;
            }
        } else {
            int pos0 = view.position();
            boolean match;
            do {
                int pos = view.position();
                match = true;
                for (int i = 0; i < text.length; i++) {
                    int b = text[i] & 0xff;
                    int c = view.nextChar();
                    if (b != c) {
                        view.position(pos);
                        match = false;
                        break;
                    }
                }
                if (match) {
                    if (matchEnd && !view.drained()) {
                        view.position(pos0);
                        return -1;
                    } else {
                        return text.length;
                    }
                }
            } while (view.nextChar() != -1);

            view.position(pos0);
            return -1;
        }
    }

    @Override
    public void visitMtns(SearchPatternNodeHandler handler) {
        handler.handleNode(this);
    }
}
