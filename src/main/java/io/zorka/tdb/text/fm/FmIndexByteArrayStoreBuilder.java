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

import java.io.IOException;

public class FmIndexByteArrayStoreBuilder implements FmIndexStoreBuilder {

    private byte[] buf;

    private int pos;

    public FmIndexByteArrayStoreBuilder(byte[] buf) {
        this.buf = buf;
        this.pos = 0;
    }

    public int getPos() {
        return pos;
    }

    @Override
    public void write(FmBlockDesc bdesc, byte[] data, int offs, int len) throws IOException {
        if (pos < buf.length) {
            System.arraycopy(data, offs, buf, pos, Math.min(len, buf.length - pos));
        }
    }

    @Override
    public void finish(int nwords, long idbase, int pidx) throws IOException {

    }
}
