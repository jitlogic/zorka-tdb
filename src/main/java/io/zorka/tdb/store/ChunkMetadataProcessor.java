package io.zorka.tdb.store;

import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.meta.ChunkMetadata;

public interface ChunkMetadataProcessor {

    void process(ChunkMetadata md, SimpleTraceStore store);

}
