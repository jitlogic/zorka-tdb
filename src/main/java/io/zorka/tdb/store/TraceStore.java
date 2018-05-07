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

package io.zorka.tdb.store;

import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.search.SearchableStore;

import java.io.Closeable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * API do trace storage.
 */
public interface TraceStore extends Closeable, SearchableStore {

    /**
     * Handles trace data coming from agents.
     *
     * @param agentUUID agent UUID
     *
     * @param sessionUUID session UUID
     *
     * @param traceUUID trace UUID (each trace must have unique ID)
     *
     * @param data Base64 encoded CBOR data.
     *
     * @param md
     */
    void handleTraceData(String agentUUID, String sessionUUID, String traceUUID, String data, ChunkMetadata md);

    /**
     * Handles incoming agent state data.
     *
     * @param agentUUID agent UUID
     *
     * @param sessionUUID session UUID
     *
     * @param data Base64 encoded CBOR data
     */
    void handleAgentData(String agentUUID, String sessionUUID, String data);

    /** Opens store. */
    void open();

    /** Returns trace UUID based on local ID. */
    String getTraceUUID(long chunkId);

    /** Returns trace duration */
    long getTraceDuration(long chunkId);

    /** */
    String getDesc(long chunkId);

    /** Returns IDs of all chunks associated with trace UUID */
    List<Long> getChunkIds(String traceUUID);

    ChunkMetadata getChunkMetadata(long chunkId);

    /**
     * Returns list of traces matching selected criteria.
     * @param query search query;
     * @return list of results
     */
    TraceSearchResult search(StoreSearchQuery query);

    /** Starts store archival process.
     *  Simple store will rebuild all indexes in compressed, read-only form.
     *  Rotating store will archive current simple store and start a new one.
     */
    void archive();

    /** Returns session UUID for a given agent. */
    String getSession(String agentUUID);

    void configure(Properties props, Executor indexerExecutor, Executor cleanerExecutor);

    void setPostproc(ChunkMetadataProcessor postproc);

    long getTstart();

    long getTstop();
}

