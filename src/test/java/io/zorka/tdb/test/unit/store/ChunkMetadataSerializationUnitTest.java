package io.zorka.tdb.test.unit.store;

import io.zorka.tdb.store.ChunkMetadata;
import org.junit.Test;

import static org.junit.Assert.*;

public class ChunkMetadataSerializationUnitTest {

    @Test
    public void testSerializeDeserializeChunkMetadata() {
        ChunkMetadata m1 = new ChunkMetadata(42L, 24L, 51L, 22L, 0);
        byte[] b = ChunkMetadata.serialize(m1);

        ChunkMetadata m2 = ChunkMetadata.deserialize(b);
        assertNotNull(m2);
    }

}
