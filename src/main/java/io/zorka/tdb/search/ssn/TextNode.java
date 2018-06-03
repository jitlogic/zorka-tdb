/*
 * Copyright 2016-2018 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
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

package io.zorka.tdb.search.ssn;

import io.zorka.tdb.util.ZicoUtil;

import java.nio.charset.Charset;

/**
 * Generalized substring search expression node. As text is represented as
 * raw sequence of bytes, no encodings/conversions really take place.
 */
public class TextNode implements StringSearchNode {

    private boolean matchStart = false, matchEnd = false;
    private byte[] text;

    public static TextNode exact(String s) {
        return new TextNode(s.getBytes(), true, true);
    }

    public TextNode(byte[] text, boolean matchStart, boolean matchEnd) {
        this.text = text;
        this.matchStart = matchStart;
        this.matchEnd = matchEnd;
    }

    public boolean isMatchStart() {
        return matchStart;
    }

    public boolean isMatchEnd() {
        return matchEnd;
    }

    public byte[] getText() {
        return text;
    }


    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TextNode)) return false;

        TextNode n = (TextNode)obj;

        return n.matchStart == matchStart
                && n.matchEnd == matchEnd
                && ZicoUtil.equals(n.text, text);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("TextNode(");
        sb.append(matchStart ? "|" : "");
        sb.append(new String(text, Charset.forName("utf8")));
        sb.append(matchEnd ? "|" : "");

        return sb.toString();
    }
}
