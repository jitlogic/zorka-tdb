/*
 * Copyright 2016-2019 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
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

import io.zorka.tdb.ZicoException;
import io.zorka.tdb.text.StructuredTextIndex;
import com.jitlogic.zorka.cbor.CborDataWriter;
import io.zorka.tdb.util.CborBufReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.jitlogic.zorka.cbor.TraceRecordFlags.*;

/**
 * Handles all data passed to
 */
public class AgentHandler implements AgentDataProcessor {

    private static final Logger log = LoggerFactory.getLogger(AgentHandler.class);

    private final static int REF_DELTA = 1024;

    private int[] stringRefs = new int[REF_DELTA];
    private int[] methodRefs = new int[REF_DELTA];

    private String sessionId;

    private SimpleTraceStore store;

    private StructuredTextIndex sindex;
    private RawTraceDataFile dataFile;
    private CborDataWriter cborWriter;

    private Map<String,TraceDataIndexer> indexerCache;

    private long lastTstamp;

    private int minDuration;

    private Random rand = new Random();

    public AgentHandler(SimpleTraceStore store, String sessionUUID) {
        // TODO zrobić po prostu referencję do danego store'a
        this.store = store;
        this.sindex = store.getTextIndex();
        this.dataFile = store.getDataFile();
        this.indexerCache = store.getIndexerCache();
        this.sessionId = sessionUUID;
        this.minDuration = Integer.parseInt(store.getProps().getProperty("ingest.min.duration", "1"));

        cborWriter = new CborDataWriter(1024 * 1024, 1024 * 1024);
    }


    int methodRef(int methodId) {
        if (methodId < methodRefs.length && methodRefs[methodId] != 0) {
            return methodRefs[methodId];
        } else {
            throw new ZicoException("Unknown method ID (not sent yet ?).");
        }
    }


    int stringRef(int stringId) {
        if (stringId < stringRefs.length && stringRefs[stringId] != 0) {
            return stringRefs[stringId];
        } else {
            throw new ZicoException("Unknown string ID (not sent yet ?): " + stringId);
        }
    }


    public synchronized void handleTraceData(byte[] data, ChunkMetadata md) {


        String tid = md.getTraceIdHex() + md.getSpanIdHex();
        TraceDataIndexer translator = indexerCache.get(tid);
        if (translator == null) {
            translator = new TraceDataIndexer();
        } else {
            // Each trace that spans onto next chunk needs to have its start offset set to 0
            for (ChunkMetadata d : translator.getTraceStackRecs()) {
                d.setStartOffs(0);
            }
        }

        synchronized (translator) {
            translator.setup(sindex, this, md.getTraceId1(), md.getTraceId2(), md.getChunkNum(), new TraceDataWriter(cborWriter), cborWriter);

            // Process trace data, translate symbol/string IDs etc.
            TraceDataReader tdr = new TraceDataReader(new CborBufReader(data), translator);
            cborWriter.reset();
            tdr.run();

            long stackSize = translator.getStackDepth();

            translator.commit();

            long dataOffs = dataFile.write(cborWriter.getBuf(), 0, cborWriter.position());

            List<ChunkMetadata> tmd = translator.getTraceMetaData();
            for (ChunkMetadata metadata : tmd) {

                if (stackSize > 0 || md.getChunkNum() > 0) {
                    metadata.markFlag(TF_CHUNK_ENABLED);
                }

                if (!metadata.hasFlag(TF_SUBMIT_TRACE) && (metadata.hasFlag(TF_DROP_TRACE)
                        || (md.getDuration() < minDuration && metadata.getStartOffs() != 0))) {
                    continue;
                }

                if (metadata.getSpanId() == 0) {
                    log.warn("Trace {} without spanID. Generated random one.", metadata.getTraceIdHex());
                    md.setSpanId(rand.nextLong());
                }

                metadata.setDataOffs(dataOffs);

                store.saveChunkMetadata(metadata);
            }

            indexerCache.remove(tid);

            if (translator.getStackDepth() > 0) {
                indexerCache.put(tid, translator);
            }
        }

        lastTstamp = System.currentTimeMillis();
    }


    public synchronized void handleAgentData(byte[] data) {
        AgentDataReader ar = new AgentDataReader(new CborBufReader(data), this);
        ar.run();
    }

    public String getSessionId() {
        return sessionId;
    }

    public long getLastTstamp() {
        return lastTstamp;
    }

    @Override
    public int defStringRef(int remoteId, String s, byte type) {
        int len = stringRefs.length;

        while (len <= remoteId) {
            len += REF_DELTA;
        }

        if (stringRefs.length != len) {
            stringRefs = Arrays.copyOf(stringRefs, len);
        }

        if (s == null || s.length() == 0) {
            s = " ";
            log.warn("Empty string ref pased to agent: " + sessionId);
        }

        stringRefs[remoteId] = sindex.addTyped(type, s.getBytes());

        return stringRefs[remoteId];
    }


    @Override
    public int defMethodRef(int remoteId, int classId, int methodId, int signatureId) {
        int len = methodRefs.length;

        while (len <= remoteId) {
            len += REF_DELTA;
        }

        if (methodRefs.length != len) {
            methodRefs = Arrays.copyOf(methodRefs, len);
        }

        methodRefs[remoteId] = sindex.addMethod(stringRef(classId), stringRef(methodId), stringRef(signatureId));

        return methodRefs[remoteId];
    }
}
