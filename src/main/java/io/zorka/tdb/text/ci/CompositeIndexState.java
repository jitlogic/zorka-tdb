package io.zorka.tdb.text.ci;

import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.text.WritableTextIndex;
import io.zorka.tdb.text.WritableTextIndex;

import java.util.Collections;
import java.util.List;

/**
 *
 */
public class CompositeIndexState {

    private final List<TextIndex> allIndexes;
    private final List<TextIndex> lookupIndexes;
    private final List<TextIndex> searchIndexes;
    private final WritableTextIndex currentIndex;

    public CompositeIndexState(
            List<TextIndex> allIndexes,
            List<TextIndex> lookupIndexes,
            List<TextIndex> searchIndexes,
            boolean archived) {
        this.allIndexes = Collections.unmodifiableList(allIndexes);
        this.lookupIndexes = Collections.unmodifiableList(lookupIndexes);
        this.searchIndexes = Collections.unmodifiableList(searchIndexes);
        // TODO wyszukiwanie bieżącego indeksu przneieść poziom wyżej (a tutaj jako parametr konstruktora)
        if (!archived && lookupIndexes.size() > 0) {
            TextIndex index = lookupIndexes.get(0);
            currentIndex = index.isWritable() ? (WritableTextIndex) index : null;
        } else {
            currentIndex = null;
        }

    }

    public WritableTextIndex getCurrentIndex() {
        return currentIndex;
    }

    public List<TextIndex> getAllIndexes() { return allIndexes; }

    public List<TextIndex> getLookupIndexes() {
        return lookupIndexes;
    }

    public List<TextIndex> getSearchIndexes() {
        return searchIndexes;
    }
}
