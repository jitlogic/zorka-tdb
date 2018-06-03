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

package io.zorka.tdb.test.support;

public class VerifyingStrGen implements TestStrGen {

    private TestStrGen src;

    private int vmin;

    private boolean verbose;

    public VerifyingStrGen(TestStrGen src, boolean strict, boolean verbose) {
        this.src = src;
        this.vmin = strict ? 32 : 3;
        this.verbose = verbose;
    }

    @Override
    public String get() {
        while (true) {
            String s = src.get();
            if (s == null) {
                return null;
            }
            boolean good = true;
            for (byte b : s.getBytes()) {
                int x = b & 0xff;
                if (x < vmin) {
                    good = false;
                    break;
                }
            }
            if (good) {
                return s;
            }
            if (verbose) {
                System.out.println("Bad string: " + s);
            }
        }
    }

    @Override
    public void reset() {
        src.reset();
    }
}
