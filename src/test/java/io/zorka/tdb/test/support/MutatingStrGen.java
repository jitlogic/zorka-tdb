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

package io.zorka.tdb.test.support;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class MutatingStrGen implements TestStrGen {

    public final static int SERIAL   = 1;   // Tries all
    public final static int ROTATING = 2;
    public final static int RANDOM   = 3;

    private final int UPCASE   = 0x01;  // Mutation: upper case
    private final int DWCASE   = 0x02;  // Mutation: lower case
    private final int UDCASE   = 0x03;  // Mutation: upper/lower case
    private final int SLASHDOT = 0x04;  // Mutation: convert slashes to dots
    private final int SHA2     = 0x08;  // Mutation: SHA2 sum, base64 encoded (simulates web tokens)

    // Remaining mutations are splitting string at subsequent positions and swapping substrings
    // When both UPCASE and DWCASE bits are enabled, we'll get alternating upcase/dwcase characters

    private TestStrGen src;
    private int mode;
    private int nsteps;

    private int step;
    private String str;

    private Random rnd = new Random();
    private MessageDigest md;


    public MutatingStrGen(TestStrGen src, int mode, int nsteps) {
        this.src = src;
        this.mode = mode;
        this.nsteps = nsteps;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // Should not happen
        }
    }


    private String mutate(String s, int step) {

        // UPCASE/DWCASE
        switch (step & 0x03) {
            case UPCASE:
                s = s.toUpperCase();
                break;
            case DWCASE:
                s = s.toLowerCase();
                break;
            case UDCASE: {
                StringBuilder sb = new StringBuilder(s.length());
                for (int i = 0; i < s.length(); i++) {
                    sb.append((i & 1) == 1
                            ? Character.toUpperCase(s.charAt(i))
                            : Character.toLowerCase(s.charAt(i)));
                }
                s = sb.toString();
                break;
            }
        }

        // SLASHDOT
        if (0 != (step & SLASHDOT)) {
            s = s.replace("/", ".");
        }

        // SHA2
        if (0 != (step & SHA2)) {
            s = DatatypeConverter.printBase64Binary(md.digest(s.getBytes()));
        }

        int ri = step >>> 4;

        if (ri != 0 && ri < s.length()) {
            s = s.substring(ri, s.length()-1) + s.substring(0, ri);
        }

        return s;
    }


    @Override
    public String get() {
        switch (mode) {
            case SERIAL: {
                String s = src.get();
                if (s == null) {
                    step++;
                    src.reset();
                    return get();
                }
                return mutate(s, step);
            }
            case ROTATING: {
                if (str == null || step >= nsteps) {
                    if (null == (str = src.get())) {
                        return null;
                    }
                    step = 0;
                }
                return mutate(str, step++);
            }
            case RANDOM: {
                String s = src.get();
                if (s == null) {
                    step++;
                    if (step < nsteps) {
                        src.reset();
                        return get();
                    } else {
                        return null;
                    }
                }
                return mutate(s, rnd.nextInt(nsteps));
            }
            default:
                return null;
        }
    }


    @Override
    public void reset() {
        step = 0;
        src.reset();
    }
}

