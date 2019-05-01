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

package io.zorka.tdb.test.unit.store;

import static com.jitlogic.zorka.cbor.CBOR.*;

import static com.jitlogic.zorka.cbor.TraceDataTags.*;
import static com.jitlogic.zorka.cbor.TraceRecordFlags.*;

import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.meta.StructuredTextIndex;
import io.zorka.tdb.store.*;
import io.zorka.tdb.test.support.ZicoTestFixture;


import java.util.List;
import java.util.UUID;

import static io.zorka.tdb.test.support.TraceTestDataBuilder.*;

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

        String sessnUUID = UUID.randomUUID().toString();

        store.handleAgentData(sessnUUID, true, agentData());

        store.handleTraceData(sessnUUID, str(
            tr(true, mid(0, 0, 0), 100, 200, 1,
                ta("XXX", "YYY"),
                tb(1500, 0, 1L)
            )).get(0),
            md(42L, 24L, 0L, 1L, 0));


        RecursiveTraceDataRetriever<TraceRecord> rtr = rtr();
        store.retrieveChunk(0, rtr);
        assertNotNull(rtr.getResult());
    }

    public final static int TICKS_IN_SECOND = 1000000000/65536;

    @Test
    public void testSubmitSimpleTraceAndCheckForFormatting() throws Exception {
        SimpleTraceStore store = createSimpleStore(1);
        store.open();

        String sessnUUID = UUID.randomUUID().toString();

        store.handleAgentData(sessnUUID, true, agentData());

        store.handleTraceData(sessnUUID, str(
            tr(true, mid(0, 0, 0), 100, 100+100*TICKS_IN_SECOND, 42,
                tb(1500, 1, 1L),
                ta(ti(TAG_STRING_REF, sid("URI")), "/my/app"),
                ta("STATUS", "200"),
                tf(TF_ERROR_MARK)
            )).get(0),
            md(42L, 24L, 0L, 1L, 0));

        ChunkMetadata md = store.getChunkMetadata(0);

        assertEquals(1, md.getRecs());
        assertEquals(42, md.getCalls());
        assertEquals(100, md.getDuration());
        assertEquals(100, store.getTraceDuration(0));
        assertTrue(md.hasFlag(TF_ERROR_MARK));
    }

    @Test
    public void testSubmitSimpleTraceRotatingSingleTrace() throws Exception {

        RotatingTraceStore store = openRotatingStore();

        String sessnUUID = UUID.randomUUID().toString();

        store.handleAgentData(sessnUUID, true, agentData());
        store.handleTraceData(sessnUUID, trc(1L, 100, 200),
                md(42L, 24L, 0L, 1L, 0));

        TraceRecord rslt = store.retrieve(42L, 24L, 1L, rtr());

        assertNotNull(rslt);
        assertEquals("void com.myapp.MyClass.myMethod()", rslt.getMethod());
        assertEquals(200, rslt.getTstop() - rslt.getTstart());
        assertEquals(200, rslt.getDuration());

        List<Long> tids = store.getChunkIds(42L, 24L, 1L);
        assertTrue(tids.size() > 0);
    }

    @Test
    public void testSubmitSimpleTracesWithRotation() throws Exception {

        RotatingTraceStore store = openRotatingStore();

        String sessnUUID = UUID.randomUUID().toString();

        store.handleAgentData(sessnUUID, true, agentData());
        store.handleTraceData(sessnUUID, trc(1L, 100, 200),
                md(42L, 24L, 0L, 1L, 0));

        store.archive();

        store.handleAgentData(sessnUUID, true, agentData());
        store.handleTraceData(sessnUUID, trc(1L, 100, 200),
                md(45L, 25L, 0L, 1L, 0));

        TraceRecord rslt1 = store.retrieve(42L, 24L, 1L, rtr());
        assertNotNull(rslt1);

        TraceRecord rslt2 = store.retrieve(45L, 25L, 1L, rtr());
        assertNotNull(rslt2);

    }

    // TODO submit, close, reopen, read

    @Test
    public void testSubmitSimpleTracesWithRotationAndReopen() throws Exception {

        RotatingTraceStore store = openRotatingStore();

        String sessnUUID = UUID.randomUUID().toString();

        store.handleAgentData(sessnUUID, true, agentData());
        store.handleTraceData(sessnUUID, trc(1L, 100, 200),
                md(42L, 24L, 0L, 1L, 0));

        store.archive();

        store.handleAgentData(sessnUUID, true, agentData());
        store.handleTraceData(sessnUUID, trc(1L, 100, 200),
                md(43L, 25L, 0L, 1L, 0));

        store.close();
        store = openRotatingStore();


        TraceRecord rslt1 = store.retrieve(42L, 24L, 1L, rtr());
        assertNotNull(rslt1);

        TraceRecord rslt2 = store.retrieve(43L, 25L, 1L, rtr());
        assertNotNull(rslt2);
    }

    @Test
    public void testRetrieveTraceAndCheckOffsets() throws Exception {
        RotatingTraceStore store = openRotatingStore();

        String sessnUUID = UUID.randomUUID().toString();

        store.handleAgentData(sessnUUID, true, agentData());

        store.handleTraceData(sessnUUID, trc2(1L,100, 200),
                md(42L, 24L, 0L, 1L, 0));

        byte [] tb0 = store.retrieveRaw(42L, 24L, 1L);
        TraceRecord tr0 = store.retrieve(42L, 24L, 1L, rtr());

        assertNotNull(tr0.getChildren());
        assertEquals(0, tr0.getPos());
        assertEquals(TAG_BASE|TAG_TRACE_START, tb0[tr0.getPos()] & 0xff);

        assertEquals(2, tr0.getChildren().size());

        TraceRecord tr1 = (TraceRecord)(tr0.getChildren().get(0));
        assertNotEquals(0, tr1.getPos());
        assertEquals(TAG_BASE|TAG_TRACE_START, tb0[tr1.getPos()] & 0xff);

        TraceRecord tr2 = (TraceRecord)(tr0.getChildren().get(1));
        assertNotEquals(0, tr2.getPos());
        assertEquals(TAG_BASE|TAG_TRACE_START, tb0[tr2.getPos()] & 0xff);
    }


    @Test
    public void testSubmitSimpleTraceSimpleStoreGenCheckDescription() throws Exception {

        SimpleTraceStore store = createSimpleStore(1);
        store.open();

        String sessnUUID = UUID.randomUUID().toString();

        store.handleAgentData(sessnUUID, true, agentData());

        store.handleTraceData(sessnUUID, str(
            tr(true, mid(0, 0, 0), 100, 200, 1,
                ta("URL", "http://127.0.0.1:8080/my/app"),
                tb(1500, 0, 1L)
            )).get(0),
            md(42L, 24L, 0L, 1L, 0));


        RecursiveTraceDataRetriever<TraceRecord> rtr = rtr();
        store.retrieveChunk(0, rtr);
        assertNotNull(rtr.getResult());
    }

    @Test
    public void testSubmitSimpleTraceWithException() throws Exception {
        SimpleTraceStore store = createSimpleStore(1);
        store.open();

        String sessnUUID = UUID.randomUUID().toString();

        store.handleAgentData(sessnUUID, true, agentData());

        store.handleTraceData(sessnUUID, str(
            tr(true, mid(0, 0, 0), 100, 200, 1,
                ta("URL", "http://127.0.0.1:8080/my/app"),
                tb(1500, 0, 1L),
                ex(1, 101, "This is error",
                    sd(100, 200, 6, 42),
                    sd(101, 201, 7, 24),
                    sd(102, 202, 8, 66)
                ))).get(0),
            md(42L, 24L, 0L, 1L, 0));


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
