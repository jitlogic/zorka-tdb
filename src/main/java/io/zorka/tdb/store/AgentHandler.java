/*
 * Copyright 2016-2018 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
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

import com.jitlogic.zorka.cbor.CborDataWriter;
import io.zorka.tdb.ZicoException;
import io.zorka.tdb.meta.*;
import io.zorka.tdb.util.CborDataReader;
import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.meta.MetadataTextIndex;
import io.zorka.tdb.meta.StructuredTextIndex;
import io.zorka.tdb.util.ZicoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Handles all data passed to
 */
public class AgentHandler implements AgentDataProcessor {

    private static final Logger log = LoggerFactory.getLogger(AgentHandler.class);

    private final static int REF_DELTA = 1024;

    private int[] stringRefs = new int[REF_DELTA];
    private int[] methodRefs = new int[REF_DELTA];


    private String agentUUID;
    private String sessionUUID;
    private int hostId;
    private int agentId;
    private long storeId;

    private SimpleTraceStore store;

    private MetadataTextIndex mindex;
    private StructuredTextIndex sindex;
    private RawTraceDataFile dataFile;

    private MetadataQuickIndex qindex;

    private ChunkMetadataProcessor postproc;

    private CborDataWriter cborWriter;

    private Map<String,TraceDataIndexer> indexerCache;

    private long lastTstamp;

    private TraceTypeResolver traceTypeResolver;

    public AgentHandler(SimpleTraceStore store, String agentUUID, String sessionUUID, TraceTypeResolver traceTypeResolver) {
        // TODO zrobić po prostu referencję do danego store'a
        this.store = store;
        this.storeId = store.getStoreId();
        this.mindex = store.getMetaIndex();
        this.sindex = store.getTextIndex();
        this.qindex = store.getQuickIndex();
        this.dataFile = store.getDataFile();
        this.indexerCache = store.getIndexerCache();
        this.postproc = store.getPostproc();
        this.agentId = sindex.addTyped(StructuredTextIndex.UUID_TYPE, agentUUID);
        this.hostId = ZicoUtil.extractUuidSeq(agentUUID);
        this.agentUUID = agentUUID;
        this.sessionUUID = sessionUUID;
        this.traceTypeResolver = traceTypeResolver;

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


    public void handleTraceData(String traceUUID, String data, ChunkMetadata md) {
        byte[] ibuf = DatatypeConverter.parseBase64Binary(data);

        TraceDataIndexer translator =  indexerCache.get(traceUUID);
        if (translator == null) {
            translator = new TraceDataIndexer(traceTypeResolver);
        }

        synchronized (translator) {

            translator.setup(sindex, this, traceUUID, md.getChunkNum(), new TraceDataWriter(cborWriter), cborWriter);

            // Process trace data, translate symbol/string IDs etc.
            TraceDataReader tdr = new TraceDataReader(new CborDataReader(ibuf), translator);
            cborWriter.reset();
            tdr.run();

            long stackSize = translator.getStackDepth();

            translator.commit();

            long dataOffs = dataFile.write(cborWriter.getBuf(), 0, cborWriter.position());

            List<ChunkMetadata> tmd = translator.getTraceMetaData();
            for (ChunkMetadata metadata : tmd) {

                if (stackSize > 0 || md.getChunkNum() > 0) {
                    metadata.markFlag(ChunkMetadata.TF_CHUNKED);
                }

                metadata.setDataOffs(dataOffs);
                metadata.setAppId(md.getAppId());
                metadata.setEnvId(md.getEnvId());
                metadata.setHostId(hostId);

                if (postproc != null) postproc.process(metadata, store);

                int slotId = qindex.add(metadata);

                if (log.isDebugEnabled()) {
                    log.debug("Indexed: " + metadata + " -> " + slotId);
                }

                mindex.addTraceChunkDesc(slotId, metadata.getTraceUUID());

                int ftid = mindex.addTextMetaData(slotId, metadata.getFids(), true);
                int ttid = mindex.addTextMetaData(slotId, metadata.getTids(), false);

                qindex.setTids(slotId, ftid, ttid);
            }

            indexerCache.remove(traceUUID);

            if (translator.getStackDepth() > 0) {
                indexerCache.put(traceUUID, translator);
            }
        }

        lastTstamp = System.currentTimeMillis();
    }


    public void handleAgentData(String data) {
        byte[] ibuf = DatatypeConverter.parseBase64Binary(data);
        AgentDataReader ar = new AgentDataReader(agentUUID, new CborDataReader(ibuf), this);
        ar.run();
    }

    public String getSessionUUID() {
        return sessionUUID;
    }

    public long getLastTstamp() {
        return lastTstamp;
    }

    public String getAgentUUID() { return agentUUID; }

    @Override
    public int defStringRef(int remoteId, String s, byte type) {
        int len = stringRefs.length;

        while (len <= remoteId) {
            len += REF_DELTA;
        }

        if (stringRefs.length != len) {
            stringRefs = Arrays.copyOf(stringRefs, len);
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

    @Override
    public void defAgentAttr(String agentUUID, String key, String val) {
        sindex.addAgentAttr(agentUUID, key, val);
    }

}
