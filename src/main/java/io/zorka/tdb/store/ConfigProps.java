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

/**
 *
 */
public class ConfigProps {

    /** Maximum number of substores in rotating trace store. */
    public static final String STORES_MAX_NUM = "store.rotate.max-num";

    /** Maximum size of simple store in rotating store (MB). */
    public static final String STORES_MAX_SIZE = "store.rotate.max-size";

    /** Session timeout (sessions not accessed for period longer than that will be discarded). */
    public static final String SESSION_TIMEOUT = "store.session-timeout";

    /** Text index WAL size (MB) */
    public static final String TIDX_WAL_SIZE = "store.text.wal-size";

    /** Metadata index WAL size (MB) */
    public static final String MIDX_WAL_SIZE = "store.meta.wal-size";

    /** Metadata Quick Index size (if exceeded, it will be extended by the same amount) */
    public static final String MIDX_QIDX_SIZE = "store.qidx.size";

    /** Maximum number of text WALs per compressed index. */
    public static final String TIDX_WAL_NUM = "store.text.wal-num";

    /** Maximum number of metadata WALs per compressed index. */
    public static final String MIDX_WAL_NUM = "store.meta.wal-num";

    public static final String IFLAGS = "store.iflags";

    public static final String DFLAGS = "store.dflags";

}
