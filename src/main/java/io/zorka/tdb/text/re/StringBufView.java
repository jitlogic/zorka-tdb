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

package io.zorka.tdb.text.re;

import java.util.Arrays;

public class StringBufView implements SearchBufView {

    private int pos;
    private byte[] data;

    public StringBufView(String s) {
        this(s, false);
    }

    public StringBufView(String s, boolean inverted) {
        this(s.getBytes(), inverted);
    }

    public StringBufView(byte[] data) {
        this(data, false);
    }

    public StringBufView(byte[] data, boolean inverted) {
        if (inverted) {
            this.data = new byte[data.length];
            for (int i = 0; i < data.length; i++) {
                this.data[data.length-i-1] = data[i];
            }
        } else {
            this.data = Arrays.copyOf(data, data.length);
        }
        pos = data.length - 1;
    }

    @Override
    public int position() {
        return pos;
    }

    @Override
    public void position(int pos) {
        this.pos = Math.max(-1, Math.min(pos, data.length-1));
    }

    @Override
    public int nextChar() {
        if (pos >= 0) {
            return data[pos--];
        } else {
            return -1;
        }
    }

    @Override
    public boolean drained() {
        return pos < 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("[");
        for (byte b : data) {
            sb.append((char) (b));
        }
        sb.append("]@");
        sb.append(pos);

        return sb.toString();
    }

    public int consumed() {
        return pos == 0 ? data.length : (data.length - pos - 1);
    }

}
