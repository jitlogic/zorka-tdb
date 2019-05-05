package io.zorka.tdb.store;

import java.io.Closeable;

public interface TraceStore extends Closeable {

    void handleTraceData(String sessionUUID, byte[] data, ChunkMetadata md);

    void handleAgentData(String sessionUUID, boolean reset, byte[] data);

    void open();

    long getTstart();

    long getTstop();
}
