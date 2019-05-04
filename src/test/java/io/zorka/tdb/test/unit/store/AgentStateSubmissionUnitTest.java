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

import io.zorka.tdb.text.StructuredTextIndex;
import io.zorka.tdb.test.support.TraceTestDataBuilder;
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.store.SimpleTraceStore;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Submitting and retrieving agent state.
 */
public class AgentStateSubmissionUnitTest extends ZicoTestFixture {

    private SimpleTraceStore store;

    @Before
    public void setupStore() throws Exception {
        store = createSimpleStore(1);
        store.open();
    }


    @Test
    public void testSubmitStringRefs() {

        String sessnUUID = UUID.randomUUID().toString();

        byte[] agentData = TraceTestDataBuilder.str(
            TraceTestDataBuilder.sr(10, "com.myapp.MyClass", TraceTestDataBuilder.TC),
            TraceTestDataBuilder.sr(11, "myMethod", TraceTestDataBuilder.TM),
            TraceTestDataBuilder.sr(12, "()V", TraceTestDataBuilder.TS),
            TraceTestDataBuilder.mr(13, 10, 11, 12)).get(0);

        store.handleAgentData(sessnUUID, true, agentData);

        int cid = store.getTextIndex().getTyped(TraceTestDataBuilder.TC, "com.myapp.MyClass");
        int mid = store.getTextIndex().getTyped(TraceTestDataBuilder.TM, "myMethod");
        int sid = store.getTextIndex().getTyped(TraceTestDataBuilder.TS, "()V");

        assertNotEquals(-1, cid);
        assertNotEquals(-1, mid);
        assertNotEquals(-1, sid);

        int mref = store.getTextIndex().getTuple(StructuredTextIndex.METHOD_DESC, cid, mid, sid);
        assertNotEquals(-1, mref);
    }

    // TODO test czy nadmiarowe mIDs się nie indeksują przypadkiem ...
}
