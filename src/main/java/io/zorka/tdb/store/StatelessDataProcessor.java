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

import java.util.Map;

/**
 * Interface for handling low level incoming trace data. This allows implementing stateless processors suitable for
 * transforming trace data with minimal memory footprint, yet some operations are cumbersome (eg. reconstruction, filtering).
 */
public interface StatelessDataProcessor {

    void traceStart(int pos);

    void traceEnd();

    void traceInfo(int k, long v);

    void attr(Map<Object,Object> data);

    void exceptionRef(int ref);

    void exception(ExceptionData ex);

    void commit();
}
