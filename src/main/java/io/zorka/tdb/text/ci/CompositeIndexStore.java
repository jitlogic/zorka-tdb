package io.zorka.tdb.text.ci;

import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.text.WritableTextIndex;
import io.zorka.tdb.text.WritableTextIndex;

import java.util.List;

/**
 * Access to index files (scanning, opening, creation etc.)
 */
public interface CompositeIndexStore {

    String getPath();

    List<TextIndex> listAll();

    WritableTextIndex addIndex(int idBase);

    TextIndex mergeIndex(List<TextIndex> indexes);

    void removeIndex(TextIndex index);

    TextIndex compressIndex(TextIndex index);
}
