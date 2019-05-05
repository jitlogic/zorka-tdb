package io.zorka.tdb.test.unit.store;

import io.zorka.tdb.store.ChunkMetadata;
import org.junit.Test;

import static org.junit.Assert.*;

public class ChunkMetadataSerializationUnitTest {

    @Test
    public void testSerializeDeserializeChunkMetadata() {
        ChunkMetadata m1 = new ChunkMetadata(42L, 24L, 51L, 22L, 99);
        m1.getSattrs().put(42, 24);
        byte[] b = ChunkMetadata.serialize(m1);


        ChunkMetadata m2 = ChunkMetadata.deserialize(b);
        assertNotNull(m2);
        assertEquals(42L, m2.getTraceId1());
        assertEquals(24L, m2.getTraceId2());
        assertEquals(51L, m2.getParentId());
        assertEquals(22L, m2.getSpanId());
        assertEquals(99, m2.getChunkNum());
        assertEquals((Integer)24, m2.getSattrs().get(42));
    }

}
