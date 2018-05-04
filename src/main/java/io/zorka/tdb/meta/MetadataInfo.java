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

package io.zorka.tdb.meta;

/**
 *
 */
public interface MetadataInfo {

    /** Retrieve also trace attibutes. */
    int SF_ATTRS = 0x01;

    /** Start with oldest traces, not newest (default) */
    int SF_REVERSE = 0x02;

    /** Search only last (active) store. */
    int SF_CURRENT = 0x04;

    /** Error flag  */
    int TF_ERROR = 0x01;

    /** If set, chunk is only part of whole trace. */
    int TF_CHUNKED = 0x02;

    /** Marks first chunk of trace. */
    int TF_INITIAL = 0x04;

    /** Marks final chunk of trace. */
    int TF_FINAL = 0x08;

    int getAppId();

    int getEnvId();

    int getTypeId();

    long getDuration();

    int getFlags();

    int getCalls();

    int getErrors();

    int getRecs();
}
