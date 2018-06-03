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

package io.zorka.tdb.text.fm;

/**
 * Predefined compression settings for FMI index builders.
 * Compression ratio as percent of original length.
 * Extraction speed benchmarked on 1.7 GHz Haswell
 * machine with 320MB of data indexed.
 */
public enum FmCompressionLevel {

    LEVEL0(512, 270,  0, 64, 8),      // ~9MB/s cr, ~4.3MB/s r, ~19%
    LEVEL1(512, 270, 90, 64, 16),     // ~7MB/s, ~14%
    LEVEL2(1024, 270, 90, 64, 32),    // ~5MB/s, ~11.75%
    LEVEL3(2048, 270, 90, 64, 64),    // ~3MB/s, ~11%
    LEVEL4(4096, 270, 90, 64, 64),    // ~1.5MB/s, ~10.5%

    DEFAULT(1024, 270, 90, 64, 32)    // same as LEVEL2
    ;

    public final int blksz;
    public final int gapsz;
    public final int crmin;
    public final int csmin;
    public final int dbgap;

    FmCompressionLevel(int blksz, int gapsz, int crmin, int csmin, int dbgap) {
        this.blksz = blksz;
        this.gapsz = gapsz;
        this.crmin = crmin;
        this.csmin = csmin;
        this.dbgap = dbgap;
    }

}

