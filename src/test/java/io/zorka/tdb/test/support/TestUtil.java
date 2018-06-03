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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;

import static org.junit.Assert.*;

public class TestUtil {

    public static byte[] readc(String path, boolean term) throws Exception {
        URL url = TestUtil.class.getResource(path);
        if (url == null) {
            throw new IOException("Cannot find classpath resource: " + path);
        }
        File f = new File(url.getPath());
        int l = (int)f.length() + (term ? 1 : 0);
        byte[] buf = new byte[l];
        FileInputStream is = new FileInputStream(f);
        is.read(buf);
        is.close();
        return buf;
    }


    public static byte[] readf(File f, int limit, boolean term) {
        return readf(f.getPath(), limit, term);
    }


    public static byte[] readf(String path, int limit, boolean term) {
        try {
            File f = new File(path);
            if (!f.canRead()) {
                throw new IOException("Cannot read file: " + path);
            }
            int l = (int) f.length() + (term ? 1 : 0);
            if (limit > 0 && l > limit) {
                l = limit;
            }
            byte[] buf = new byte[l];
            FileInputStream is = new FileInputStream(f);
            is.read(buf);
            is.close();
            if (term) {
                buf[l - 1] = 0;
            }
            return buf;
        } catch (IOException e) {
            fail("Cannot open input file " + path + ": " + e.getMessage());
            return new byte[0];
        }
    }


    public static void writef(String path, byte[] ... bufs) throws Exception {
        FileOutputStream os = new FileOutputStream(path);
        for (byte[] buf : bufs) {
            os.write(buf);
        }
        os.close();
    }


    public static void assertEquals(byte[] b1, byte[] b2) {
        assertEquals("", b1, b2);
    }

    public static void printBytes(String name, byte[] b) {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append(" = [");
        for (int i = 0; i < b.length; i++) {
            if (i != 0) sb.append(", ");
            sb.append(b[i] & 0xff);
        }
        sb.append("]");
        System.err.println(sb);
    }

    public static void assertEquals(String msg, byte[] b1, byte[] b2) {
        if (b1 == b2) return;
        if (b1 == null) return;  // TODO WTF ?

        if (b1.length != b2.length) {
            printBytes("b1", b1);
            printBytes("b2", b2);
            fail(msg + "  len(b1)=" + b1.length + ", len(b2)=" + b2.length);
        }

        for (int i = 0; i < b1.length; i++) {
            if (b1[i] != b2[i]) {
                printBytes("b1", b1);
                printBytes("b2", b2);
                fail(msg + "  b1[" + i + "]=" + b1[i] + ", b2[" + i + "]=" + b2[i]);
            }
        }
    }


    public static void arraysEqual(String msg, byte[] b1, byte[] b2, int limit) {
        if (b1 == b2) return;

        if (b1 == null || b1.length < limit) fail("b1 null or too short.");
        if (b2 == null || b2.length < limit) fail("b2 null or too short.");

        for (int i = 0; i < limit; i++) {
            if (b1[i] != b2[i])
                fail("Arrays differ at position " + i + ": " + (b1[i] & 0xff) + " <-> " + (b2[i] & 0xff));
        }
    }


    public static String toString(byte[] ba) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (byte b : ba) {
            if (sb.length() > 1) {
                sb.append(", ");
            }
            sb.append(b & 0xff);
        }
        sb.append(']');
        return sb.toString();
    }


    public static void printHistogram(byte[] buf, int n, String prefix) {
        long[] histogram = new long[256];
        for (int i = 0; i < n; i++) {
            histogram[buf[i] & 0xff]++;
        }

        int zs = 0, iof = -1;

        for (int i = 0; i < histogram.length; i++) {
            long r = histogram[i] * 256 * 256 / n;
            if (r == 0) zs += histogram[i];
            if (r == 0 && iof == -1) iof = i;
            System.out.println(prefix + "[" + i + "] = " + histogram[i] + "  -  " + r);
        }



        System.out.println("Remaining: " + zs * 256 / n);
    }


    public static void rmrf(String path) throws IOException {
        rmrf(new File(path));
    }


    public static void rmrf(File f) throws IOException {
        if (f.exists()) {
            if (f.isDirectory()) {
                for (File c : f.listFiles()) {
                    rmrf(c);
                }
            }
            f.delete();
        }
    }


    public static String path(String...components) {
        File f = new File(components[0]);
        for (int i = 1; i < components.length; i++) {
            f = new File(f, components[i]);
        }
        return f.getPath();
    }


    public static void setField(Object obj, String name, Object val) throws Exception {
        Field field = null;

        for (Field f : obj.getClass().getDeclaredFields()) {
            if (name.equals(f.getName())) {
                field = f;
                break;
            }
        }

        assertNotNull(field);

        if (!field.isAccessible())
            field.setAccessible(true);

        field.set(obj, val);
    }


    public static void setFields(Object obj, Object...args) throws Exception {
        for (int i = 1; i < args.length; i += 2) {
            assertTrue(args[i-1] instanceof String);
            setField(obj, (String)args[i-1], args[i]);
        }
    }


}
