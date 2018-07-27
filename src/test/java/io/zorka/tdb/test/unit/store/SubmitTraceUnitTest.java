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

package io.zorka.tdb.test.unit.store;

import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.meta.StructuredTextIndex;
import io.zorka.tdb.store.*;
import io.zorka.tdb.test.support.ZicoTestFixture;

import io.zorka.tdb.util.CBOR;

import java.util.List;
import java.util.UUID;

import static io.zorka.tdb.test.support.TraceTestDataBuilder.*;

import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.meta.StructuredTextIndex;
import io.zorka.tdb.store.RecursiveTraceDataRetriever;
import io.zorka.tdb.store.RotatingTraceStore;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 */
public class SubmitTraceUnitTest extends ZicoTestFixture {


    @Test
    public void testSubmitSimpleTraceSimpleStore() throws Exception {

        SimpleTraceStore store = createSimpleStore(1);
        store.open();

        String agentUUID = UUID.randomUUID().toString();
        String sessnUUID = store.getSession(agentUUID);
        String traceUUID = UUID.randomUUID().toString();

        store.handleAgentData(agentUUID, sessnUUID, agentData());

        store.handleTraceData(agentUUID, sessnUUID, traceUUID, str(
            tr(true, mid(0, 0, 0), 100, 200, 1,
                ta("XXX", "YYY"),
                tb(1500, 0)
            )).get(0),
            md(1, 2));


        RecursiveTraceDataRetriever<TraceRecord> rtr = rtr();
        store.retrieveChunk(0, rtr);
        assertNotNull(rtr.getResult());

        ChunkMetadata md = store.getChunkMetadata(0);
        assertTrue(md.getMethodId() != 0);
        //assertTrue(md.getDescId() != 0);
    }

    @Test
    public void testSubmitTraceWithDtraceAttributes() throws Exception {
        SimpleTraceStore store = createSimpleStore(1);
        store.open();

        String agentUUID = UUID.randomUUID().toString();
        String sessnUUID = store.getSession(agentUUID);
        String traceUUID = UUID.randomUUID().toString();

        store.handleAgentData(agentUUID, sessnUUID, agentData());

        store.handleTraceData(agentUUID, sessnUUID, traceUUID, str(
                tr(true, mid(0, 0, 0), 100, 200, 1,
                        tb(1500, 0),
                        ta("DTRACE_UUID", "1234-5678-9012"),
                        ta("DTRACE_IN", "1234-5678-9012/22/33")
                )).get(0),
                md(1, 2));


        RecursiveTraceDataRetriever<TraceRecord> rtr = rtr();
        store.retrieveChunk(0, rtr);
        assertNotNull(rtr.getResult());

        ChunkMetadata md = store.getChunkMetadata(0);
        assertTrue(md.getMethodId() != 0);
        assertTrue("Distributed trace UUID ref should be recorded in quick index.", md.getDtraceUUID() != 0);
        assertEquals("1234-5678-9012", store.getTextIndex().gets(md.getDtraceUUID()));
        assertTrue("Distributed trace TID ref should be recorded in quick index.", md.getDtraceTID() != 0);
        assertEquals("1234-5678-9012/22/33", store.getTextIndex().gets(md.getDtraceTID()));
    }

    @Test
    public void testSubmitSimpleTraceAndCheckForFormatting() throws Exception {
        SimpleTraceStore store = createSimpleStore(1);
        store.open();

        TemplatingMetadataProcessor tmp = new TemplatingMetadataProcessor();
        tmp.putTemplate(1, "${URI} -> ${STATUS}");
        store.setPostproc(tmp);

        String agentUUID = UUID.randomUUID().toString();
        String sessnUUID = store.getSession(agentUUID);
        String traceUUID = UUID.randomUUID().toString();

        store.handleAgentData(agentUUID, sessnUUID, agentData());

        store.handleTraceData(agentUUID, sessnUUID, traceUUID, str(
            tr(true, mid(0, 0, 0), 100, 100+100*TraceDataFormat.TICKS_IN_SECOND, 42,
                tb(1500, 1),
                ta(ti(TraceDataFormat.TAG_STRING_REF, sid("URI")), "/my/app"),
                ta("STATUS", "200"),
                tf(TraceDataFormat.FLAG_ERROR)
            )).get(0),
            md(1, 2));

        ChunkMetadata md = store.getChunkMetadata(0);
        assertTrue(md.getDescId() != 0);
        assertEquals("/my/app -> 200", store.getTextIndex().resolve(md.getDescId()));

        assertEquals(1, md.getRecs());
        assertEquals(42, md.getCalls());
        assertEquals(100, md.getDuration());
        assertEquals(100, store.getTraceDuration(0));
        assertTrue(md.hasFlag(TraceDataFormat.TF_ERROR));
    }

    @Test
    public void testSubmitSimpleTraceRotatingSingleTrace() throws Exception {

        RotatingTraceStore store = openRotatingStore();

        String agentUUID = UUID.randomUUID().toString();
        String sessnUUID = store.getSession(agentUUID);
        String traceUUID = UUID.randomUUID().toString();


        store.handleAgentData(agentUUID, sessnUUID, agentData());
        store.handleTraceData(agentUUID, sessnUUID, traceUUID, trc(100, 200), md(1, 2));

        TraceRecord rslt = store.retrieve(traceUUID, rtr());

        assertNotNull(rslt);
        assertEquals("void com.myapp.MyClass.myMethod()", rslt.getMethod());
        assertEquals(200, rslt.getTstop() - rslt.getTstart());
        assertEquals(200, rslt.getDuration());

        List<Long> tids = store.getChunkIds(traceUUID);
        assertTrue(tids.size() > 0);

        String uuid = store.getTraceUUID(tids.get(0));
        assertEquals(traceUUID, uuid);
    }

    @Test
    public void testSubmitSimpleTracesWithRotation() throws Exception {

        RotatingTraceStore store = openRotatingStore();

        String agentUUID = UUID.randomUUID().toString();
        String sessnUUID = store.getSession(agentUUID);
        String traceUUID1 = UUID.randomUUID().toString();
        String traceUUID2 = UUID.randomUUID().toString();

        store.handleAgentData(agentUUID, sessnUUID, agentData());
        store.handleTraceData(agentUUID, sessnUUID, traceUUID1, trc(100, 200), md(1, 2));

        store.archive();

        sessnUUID = store.getSession(agentUUID);
        store.handleAgentData(agentUUID, sessnUUID, agentData());
        store.handleTraceData(agentUUID, sessnUUID, traceUUID2, trc(100, 200), md(1, 2));

        TraceRecord rslt1 = store.retrieve(traceUUID1, rtr());
        assertNotNull(rslt1);

        TraceRecord rslt2 = store.retrieve(traceUUID2, rtr());
        assertNotNull(rslt2);

    }

    // TODO submit, close, reopen, read

    @Test
    public void testSubmitSimpleTracesWithRotationAndReopen() throws Exception {

        RotatingTraceStore store = openRotatingStore();

        String agentUUID = UUID.randomUUID().toString();
        String sessnUUID = store.getSession(agentUUID);
        String traceUUID1 = UUID.randomUUID().toString();
        String traceUUID2 = UUID.randomUUID().toString();

        store.handleAgentData(agentUUID, sessnUUID, agentData());
        store.handleTraceData(agentUUID, sessnUUID, traceUUID1, trc(100, 200), md(1, 2));

        store.archive();

        sessnUUID = store.getSession(agentUUID);
        store.handleAgentData(agentUUID, sessnUUID, agentData());
        store.handleTraceData(agentUUID, sessnUUID, traceUUID2, trc(100, 200), md(1, 2));

        store.close();
        store = openRotatingStore();


        TraceRecord rslt1 = store.retrieve(traceUUID1, rtr());
        assertNotNull(rslt1);

        TraceRecord rslt2 = store.retrieve(traceUUID2, rtr());
        assertNotNull(rslt2);
    }

    @Test
    public void testRetrieveTraceAndCheckOffsets() throws Exception {
        RotatingTraceStore store = openRotatingStore();

        String agentUUID = UUID.randomUUID().toString();
        String sessnUUID = store.getSession(agentUUID);
        String traceUUID1 = UUID.randomUUID().toString();

        store.handleAgentData(agentUUID, sessnUUID, agentData());

        store.handleTraceData(agentUUID, sessnUUID, traceUUID1, trc2(100, 200), md(1, 2));

        byte [] tb0 = store.retrieveRaw(traceUUID1);
        TraceRecord tr0 = store.retrieve(traceUUID1, rtr());

        assertNotNull(tr0.getChildren());
        assertEquals(0, tr0.getPos());
        assertEquals(CBOR.TAG_BASE | TraceDataFormat.TAG_TRACE_START, tb0[tr0.getPos()] & 0xff);

        assertEquals(2, tr0.getChildren().size());

        TraceRecord tr1 = (TraceRecord)(tr0.getChildren().get(0));
        assertNotEquals(0, tr1.getPos());
        assertEquals(CBOR.TAG_BASE | TraceDataFormat.TAG_TRACE_START, tb0[tr1.getPos()] & 0xff);

        TraceRecord tr2 = (TraceRecord)(tr0.getChildren().get(1));
        assertNotEquals(0, tr2.getPos());
        assertEquals(CBOR.TAG_BASE | TraceDataFormat.TAG_TRACE_START, tb0[tr2.getPos()] & 0xff);
    }


    @Test
    public void testSubmitSimpleTraceSimpleStoreGenCheckDescription() throws Exception {

        SimpleTraceStore store = createSimpleStore(1);
        store.open();

        String agentUUID = UUID.randomUUID().toString();
        String sessnUUID = store.getSession(agentUUID);
        String traceUUID = UUID.randomUUID().toString();

        store.handleAgentData(agentUUID, sessnUUID, agentData());

        store.handleTraceData(agentUUID, sessnUUID, traceUUID, str(
            tr(true, mid(0, 0, 0), 100, 200, 1,
                ta("URL", "http://127.0.0.1:8080/my/app"),
                tb(1500, 0)
            )).get(0),
            md(1, 2));


        RecursiveTraceDataRetriever<TraceRecord> rtr = rtr();
        store.retrieveChunk(0, rtr);
        assertNotNull(rtr.getResult());

        ChunkMetadata md = store.getChunkMetadata(0);
        assertTrue(md.getMethodId() != 0);
        //assertTrue(md.getDescId() != 0);
    }

    @Test
    public void testSubmitSimpleTraceWithException() throws Exception {
        SimpleTraceStore store = createSimpleStore(1);
        store.open();

        String agentUUID = UUID.randomUUID().toString();
        String sessnUUID = store.getSession(agentUUID);
        String traceUUID = UUID.randomUUID().toString();

        store.handleAgentData(agentUUID, sessnUUID, agentData());

        store.handleTraceData(agentUUID, sessnUUID, traceUUID, str(
            tr(true, mid(0, 0, 0), 100, 200, 1,
                ta("URL", "http://127.0.0.1:8080/my/app"),
                tb(1500, 0),
                ex(1, 101, "This is error",
                    sd(100, 200, 6, 42),
                    sd(101, 201, 7, 24),
                    sd(102, 202, 8, 66)
                ))).get(0),
            md(1, 2));


        RecursiveTraceDataRetriever<TraceRecord> rtr = rtr();
        store.retrieveChunk(0, rtr);
        TraceRecord tr = rtr.getResult();

        assertNotNull(tr);

        assertNotEquals(0, tr.getEid());

        StructuredTextIndex ix = store.getTextIndex();
        ExceptionData ed = ix.getExceptionData(tr.getEid(), true);

        assertNotNull(ed);

        assertEquals("org.catalina.request.Request", ix.resolve(ed.getClassId()));
        assertEquals("This is error", ix.resolve(ed.getMsgId()));
        List<StackData> sts = ed.getStackTrace();
        assertEquals(3, sts.size());

        StackData sd0 = sts.get(0);
        assertEquals("com.myapp.MyClass", ix.resolve(sd0.getClassId()));
        assertEquals("myMethod", ix.resolve(sd0.getMethodId()));
        assertEquals("MyClass.java", ix.resolve(sd0.getFileId()));
        assertEquals(42, sd0.getLineNum());
    }

}
