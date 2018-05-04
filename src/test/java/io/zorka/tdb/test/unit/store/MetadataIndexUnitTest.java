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

import io.zorka.tdb.meta.MetadataTextIndex;
import io.zorka.tdb.test.support.ZicoTestFixture;

import io.zorka.tdb.text.ci.CompositeIndex;
import io.zorka.tdb.text.ci.CompositeIndexFileStore;
import io.zorka.tdb.util.ZicoUtil;
import org.junit.Ignore;
import org.junit.Test;


/**
 *
 */
public class MetadataIndexUnitTest extends ZicoTestFixture {


    @Test @Ignore("Fix me.")
    public void testAddAndListMetadata() throws Exception {
        CompositeIndex cti = new CompositeIndex(new CompositeIndexFileStore(tmpDir, "test", ZicoUtil.props()), ZicoUtil.props(), Runnable::run);
        MetadataTextIndex mti = new MetadataTextIndex(cti);

        // TODO proper unit testing for adding / listing trace metadata
//        mti.addTextMetaData(md(1, 2, 3, 4, 5, 100, 200,
//            false, 100, 0));
//        mti.addTextMetaData(md(2, 2, 3, 4, 5, 100, 200,
//            false, 200, 0));
//        mti.addTextMetaData(md(3, 2, 3, 4, 5, 100, 200,
//            false, 300, 0));

        cti.close();
    }

}
