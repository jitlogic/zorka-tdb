package io.zorka.tdb.search.ssn;

import java.util.BitSet;

/**
 * Matches by character class. This is intended to be used in bigger logical constructs
 * as searching for single characters across vast stores does not make sense.
 * Note that this works only on bytes, thus character classes are valid only for ASCII characters.
 */
public class CharClassNode implements StringSearchNode {

    private BitSet chars = new BitSet();

    public void addRange(char first, char last) {
        addRange((int)first, (int)last);
    }

    public void addRange(int first, int last) {
        chars.set(first, last+1);
    }

    public BitSet getChars() {
        return chars;
    }

    @Override
    public String toString() {
        return "CharClassNode(" + chars + ")";
    }
}

