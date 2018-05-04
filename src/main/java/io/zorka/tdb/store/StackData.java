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

package io.zorka.tdb.store;

/**
 *
 */
public class StackData {
    int classId;

    int methodId;

    int fileId;

    int lineNum;

    public StackData(int classId, int methodId, int fieldId, int lineNum) {
        this.classId = classId;
        this.methodId = methodId;
        this.fileId = fieldId;
        this.lineNum = lineNum;
    }

    public int getClassId() {
        return classId;
    }

    public int getMethodId() {
        return methodId;
    }

    public int getFileId() {
        return fileId;
    }

    public int getLineNum() {
        return lineNum;
    }

    @Override
    public String toString() {
        return "[c=" + classId + ", m=" + methodId + ", f=" + fileId + ", l=" + lineNum + "]";
    }


}
