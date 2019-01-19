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

import java.util.List;

public class TraceStoreUtil {

    public final static long SLOT_MASK     = 0x000000007fffffffL;
    public final static long CH_START_FLAG = 0x8000000000000000L;
    public final static long CH_END_FLAG   = 0x0000000080000000L;
    public final static long CH_SE_MASK    = 0x7fffffff7fffffffL;

    public static int parseStoreId(long chunkId) {
        return (int)((chunkId >>> 32) & SLOT_MASK);
    }

    public static int parseSlotId(long chunkId) {
        return (int)(chunkId & SLOT_MASK);
    }

    public static boolean parseStartFlag(long chunkId) {
        return 0 != (chunkId & CH_START_FLAG);
    }

    public static boolean parseEndFlag(long chunkId) {
        return 0 != (chunkId & CH_END_FLAG);
    }

    public static long formatChunkId(int storeId, int slotId, boolean startFlag, boolean endFlag) {
        return (((long)storeId) << 32) | slotId | (startFlag ? CH_START_FLAG : 0) | (endFlag ? CH_END_FLAG : 0);
    }

    public static boolean containsStartChunk(List<Long> chunks) {
        return chunks.stream().filter(TraceStoreUtil::parseStartFlag).count() > 0;
    }

}
