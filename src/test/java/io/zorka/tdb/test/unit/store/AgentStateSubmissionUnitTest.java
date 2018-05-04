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

package io.zorka.tdb.test.unit.store;

import io.zorka.tdb.meta.StructuredTextIndex;
import io.zorka.tdb.store.SimpleTraceStore;
import io.zorka.tdb.test.support.ZicoTestFixture;

import java.util.Arrays;
import java.util.UUID;

import static io.zorka.tdb.util.ZicoUtil.map;

import static io.zorka.tdb.test.support.TraceTestDataBuilder.*;

import io.zorka.tdb.meta.StructuredTextIndex;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 */
public class AgentStateSubmissionUnitTest extends ZicoTestFixture {

    private SimpleTraceStore store;

    @Before
    public void setupStore() throws Exception {
        store = createSimpleStore(1);
        store.open();
    }


    @Test
    public void testSubmitAgentAttrs() {
        assertEquals(24, store.length());

        String agentUUID = UUID.randomUUID().toString();
        String sessnUUID = store.getSession(agentUUID);
        String agentData = str(aa("ENV", "UAT"), aa("APP", "PETSTORE")).get(0);

        store.handleAgentData(agentUUID, sessnUUID, agentData);

        assertEquals(
            map("ENV", "UAT", "APP", "PETSTORE", "_UUID", agentUUID),
            store.getTextIndex().getAgentInfo(agentUUID));

        assertEquals(
            map(agentUUID, Arrays.asList("APP", "ENV")),
            store.getTextIndex().getAgentAttrs());
    }


    @Test
    public void testSubmitStringRefs() {

        String agentUUID = UUID.randomUUID().toString();
        String sessnUUID = store.getSession(agentUUID);

        String agentData = str(
            sr(10, "com.myapp.MyClass", TC),
            sr(11, "myMethod", TM),
            sr(12, "()V", TS),
            mr(13, 10, 11, 12)).get(0);

        store.handleAgentData(agentUUID, sessnUUID, agentData);

        int cid = store.getTextIndex().getTyped(TC, "com.myapp.MyClass");
        int mid = store.getTextIndex().getTyped(TM, "myMethod");
        int sid = store.getTextIndex().getTyped(TS, "()V");

        assertNotEquals(-1, cid);
        assertNotEquals(-1, mid);
        assertNotEquals(-1, sid);

        int mref = store.getTextIndex().getTuple(StructuredTextIndex.METHOD_DESC, cid, mid, sid);
        assertNotEquals(-1, mref);
    }

    // TODO test czy nadmiarowe mIDs się nie indeksują przypadkiem ...
}
