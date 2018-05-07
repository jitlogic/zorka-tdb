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
