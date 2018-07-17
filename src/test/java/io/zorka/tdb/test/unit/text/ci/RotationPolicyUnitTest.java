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

package io.zorka.tdb.test.unit.text.ci;

import io.zorka.tdb.test.support.ZicoTestFixture;

import io.zorka.tdb.text.ci.*;
import io.zorka.tdb.util.ZicoUtil;
import io.zorka.tdb.text.TextIndex;
import io.zorka.tdb.text.ci.CompositeIndexState;
import io.zorka.tdb.text.ci.CompositeIndexStore;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;
import static io.zorka.tdb.test.unit.text.ci.CompositeIndexTestUtil.*;

import java.util.List;
import java.util.Properties;

public class RotationPolicyUnitTest extends ZicoTestFixture {

    @Test
    public void testFindLookupIndexes() {
        CompositeIndex rs = new CompositeIndex(BS(), ZicoUtil.props(), new TestExecutor());
        assertEquals(I(), rs.findLookupIndexes(I()));
        assertEquals(I("W1"), rs.findLookupIndexes(I("W1")));
        assertEquals(I("W11", "R1"), rs.findLookupIndexes(I("R1", "W11")));
        assertEquals(I("W11", "W1"), rs.findLookupIndexes(I("R1", "W1", "W11")));
        assertEquals(I("W21", "R1,20"), rs.findLookupIndexes(I("R1,20", "R1", "R11", "W21")));
        assertEquals(I("W21", "R1", "R11"), rs.findLookupIndexes(I("R1", "R11", "W21")));
        assertEquals(I("W21", "R1", "R11"), rs.findLookupIndexes(I("W21", "R1", "R11")));
        // TODO cases with fully archived composite index (no writable ones)
    }


    @Test
    public void testFindSearchIndexes() {
        CompositeIndex rs = new CompositeIndex(BS(), ZicoUtil.props(), new TestExecutor());
        assertEquals(I(), rs.findSearchIndexes(I()));
        assertEquals(I("W1"), rs.findSearchIndexes(I("W1")));
        assertEquals(I("W11", "R1"), rs.findSearchIndexes(I("R1", "W11")));
        assertEquals(I("W11", "R1"), rs.findSearchIndexes(I("R1", "W1", "W11")));
        assertEquals(I("W21", "R1,20"), rs.findSearchIndexes(I("R1,20", "R1", "R11", "W21")));
        assertEquals(I("W21", "R11", "R1"), rs.findSearchIndexes(I("R1", "R11", "W21")));
        assertEquals(I("W11", "W1"), rs.findSearchIndexes(I("W1", "W11")));
        // TODO cases with fully archived composite index (no writable ones)
    }


    @Test
    public void testFindCompressIndexes() {
        CompositeIndex rs = new CompositeIndex(BS(), ZicoUtil.props(), new TestExecutor());
        assertEquals(I(), rs.findCompressIndexes(I(), false));
        assertEquals(I(), rs.findCompressIndexes(I("W1"), false));
        assertEquals(I("W1"), rs.findCompressIndexes(I("W1"), true));
        assertEquals(I("W11"), rs.findCompressIndexes(I("R1", "W1", "W11", "W21"), false));
    }


    @Test
    public void testRemoveFmIndexes() {
        CompositeIndex rs = new CompositeIndex(BS(), ZicoUtil.props(), new TestExecutor());
        assertEquals(I(), rs.findRemoveFmIndexes(I()));
        assertEquals(I(), rs.findRemoveFmIndexes(I("W1")));
        assertEquals(I("R1", "R11"), rs.findRemoveFmIndexes(I("R1,20", "R1", "R11")));
    }


    @Test
    public void testRemoveWalIndexes() {
        // TODO test it after refactor
        CompositeIndex rs = new CompositeIndex(BS(), ZicoUtil.props(), new TestExecutor());
        assertEquals(I(), rs.findRemoveWalIndexes(I()));
        assertEquals(I("W1"), rs.findRemoveWalIndexes(I("R1", "W1", "W11")));
    }


    @Test
    public void testSizeToGenCalculation() {
        CompositeIndex bs = new CompositeIndex(BS(), ZicoUtil.props(), new TestExecutor());
        assertEquals(0, bs.toGen(5, 10));
        assertEquals(1, bs.toGen(10, 10));
        assertEquals(1, bs.toGen(15, 10));
        assertEquals(2, bs.toGen(20, 10));
        assertEquals(2, bs.toGen(39, 10));
        assertEquals(3, bs.toGen(40, 10));
        assertEquals(4, bs.toGen(100, 10));
        assertEquals(7, bs.toGen(1000, 10));
        assertEquals(10, bs.toGen(10000, 10));
    }


    @Test
    public void testFindMergeCandidates() {
        CompositeIndex bs = new CompositeIndex(BS(),
                ZicoUtil.props("rotation.base_size", "1", "rotation.max_gens", "4"),
                new TestExecutor());
        assertNull(bs.findMergeCandidate(I("R1", "W11")));
        assertEquals(I("R1", "R11"), bs.findMergeCandidate(I("R1", "R11", "W11")));
        assertNull(bs.findMergeCandidate(I("R1,10,2048", "R11")));
        assertEquals(I("R1,10,2048", "R11,10,2048"),
                bs.findMergeCandidate(I("R1,10,2048","R11,10,2048","R21,10,1024")));
        assertEquals(I("R1,10,1024", "R11,10,2048"),
                bs.findMergeCandidate(I("R1,10,1024", "R11,10,2048")));
    }


    private void testInitState(CompositeIndexStore store, boolean runMaintenance, TextIndex c,
                               List<TextIndex> lidx, List<TextIndex> sidx) {
        TestExecutor executor = new TestExecutor();
        CompositeIndex index = new CompositeIndex(store, new Properties(), executor);

        if (runMaintenance) index.runMaintenance();

        CompositeIndexState state = index.getCState();
        assertNotNull("Index should return some state.", state);
        Assert.assertEquals("Current index not valid", c, state.getCurrentIndex());

        for (TextIndex ix : state.getLookupIndexes()) {
            assertTrue("Index " + ix + " should be open.", ix.isOpen());
        }

        for (TextIndex ix : state.getSearchIndexes()) {
            assertTrue("Index " + ix + " should be open.", ix.isOpen());
        }

        for (TextIndex ix : state.getAllIndexes()) {
            assertTrue("Index " + ix + " shoud be open.", ix.isOpen());
        }

        assertEquals("lookupIndexes do not match", lidx, state.getLookupIndexes());
        assertEquals("searchIndexes do not match", sidx, state.getSearchIndexes());
        assertEquals(1, executor.getTasks().size());



    }


    @Test
    public void testInitState() {
        // Single WAL file
        testInitState(BS("W1"), false, i("W1"), I("W1"), I("W1"));

        // Archived FM file and one WAL file
        testInitState(BS("R1", "W11"), false, i("W11"), I("W11", "R1"), I("W11", "R1"));

        // Archived FM file, WAL with the same ID base,
        testInitState(BS("R1", "W1", "W11"), false, i("W11"), I("W11", "W1"), I("W11", "R1"));

        // 2-nd level FM, FM, FM, WAL
        testInitState(BS("R1,20", "R1", "R11", "W21"), false, i("W21"), I("W21", "R1,20"), I("W21", "R1,20"));

        // 2-nd level with overlapping FM and WAL, no maintenance
        testInitState(BS("W1", "W11", "W21", "R1,20"), false, i("W21"), I("W21", "W11", "W1"), I("W21", "R1,20"));
    }

    @Test @Ignore("Fix this.")
    public void testInitStateAfterMaintenance() {
        // 2-nd level with overlapping FM and WAL, with maintenance
        testInitState(BS("W1", "W11", "W21", "R1,20"), true, i("W21"), I("W21", "W11", "R1,20"), I("W21", "R1,20"));
    }

    @Test
    public void testAddRecordWithEmptyIndexWithoutWalFiles() {
        TestExecutor executor = new TestExecutor();
        CompositeIndex idx = new CompositeIndex(BS() ,ZicoUtil.props(), executor);
        executor.doAll();
        int id1 = idx.add("aaa"), id2 = idx.add("bbb");
        assertNotEquals(-1, id1);
        assertNotEquals(-1, id2);
    }

    @Test
    public void testAddRecordWithSingleWalRotation() {
        TestExecutor executor = new TestExecutor();
        CompositeIndex idx = new CompositeIndex(BS("W1"), ZicoUtil.props(), executor);
        executor.doAll();
        int id1 = idx.add("aaa"), id2 = idx.add("bbb");
        assertNotEquals("Should receive unique identifiers.", id1, id2);
        assertNotEquals(-1, id1);
        assertNotEquals(-1, id2);

        ((TextIndexW)(idx.getCState().getCurrentIndex())).setForceRotate(true);
        TextIndex idx1 = idx.getCState().getCurrentIndex();
        int id3 = idx.add("aaa");
        assertEquals(id3, id1);

        int id4 = idx.add("ccc");
        assertTrue(id4 > id3);
        TextIndex idx2 = idx.getCState().getCurrentIndex();

        assertNotEquals(idx1, idx2);
        assertTrue(executor.getTasks().size() > 0);

        assertTrue("Prior to compression last index in search indexes should point to WAL",
                idx.getCState().getSearchIndexes().get(1).isWritable());

        executor.doAll();

        assertFalse("After compression last index in search indexes should point to FMI",
                idx.getCState().getSearchIndexes().get(1).isWritable());
    }

    @Test
    public void addRecordsWithMerge() {
        TestExecutor executor = new TestExecutor();
        CompositeIndex idx = new CompositeIndex(BS("R1", "W11"), ZicoUtil.props(), executor);
        executor.doAll();

        ((TextIndexW)(idx.getCState().getCurrentIndex())).setForceRotate(true);
        idx.add("a");

        executor.doAll();
        //assertEquals(I("W21,10,1024", "R1,21,2048"), idx.getCState().getSearchIndexes());
    }

}
