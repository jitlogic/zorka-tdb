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

package io.zorka.tdb.util;

import org.objectweb.asm.Type;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.util.*;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

public class ZicoUtil {

    /**
     * Equivalent of a.equals(b) handling edge cases (if a and/or b is null).
     *
     * @param a compared object
     * @param b compared object
     * @return true if both a and b are null or a equals b
     */
    public static boolean objEquals(Object a, Object b) {
        return a == null && b == null
            || a != null && a.equals(b);
    }


    private static final char[] HEX = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    public static String hex(byte[] input) {
        return hex(input, input.length);
    }

    public static String hex(byte[] input, int len) {
        StringBuilder sb = new StringBuilder(input.length * 2);

        for (int i = 0; i < len; i++) {
            int c = input[i] & 0xff;
            sb.append(HEX[(c >> 4) & 0x0f]);
            sb.append(HEX[c & 0x0f]);
        }

        return sb.toString();
    }



    /**
     * This is useful to create a map of object in declarative way. Key-value pairs
     * are passed as arguments to this method, so call will look like this:
     * ZorkaUtil.map(k1, v1, k2, v2, ...)
     *
     * @param data keys and values (in pairs)
     * @param <K>  type of keys
     * @param <V>  type of values
     * @return mutable map
     */
    public static <K, V> HashMap<K, V> map(Object... data) {
        HashMap<K, V> map = new HashMap<K, V>(data.length + 2);

        for (int i = 1; i < data.length; i += 2) {
            map.put((K) data[i - 1], (V) data[i]);
        }

        return map;
    }

    public static <K,V> Map<K,V> i2map(Iterable<Map.Entry<K,V>> source) {
        Map<K,V> m = new HashMap<>();
        for (Map.Entry<K,V> e : source) {
            m.put(e.getKey(), e.getValue());
        }
        return m;
    }

    /**
     * This is useful to create a map of object in declarative way. Key-value pairs
     * are passed as arguments to this method, so call will look like this:
     * ZorkaUtil.map(k1, v1, k2, v2, ...)
     *
     * @param data keys and values (in pairs)
     * @param <K>  type of keys
     * @param <V>  type of values
     * @return mutable map
     */
    public static <K, V> Map<K, V> lmap(Object... data) {
        Map<K, V> map = new LinkedHashMap<K, V>(data.length + 2);

        for (int i = 1; i < data.length; i += 2) {
            map.put((K) data[i - 1], (V) data[i]);
        }

        return map;
    }


    /**
     * This is useful to create a map of object in declarative way. Key-value pairs
     * are passed as arguments to this method, so call will look like this:
     * ZorkaUtil.map(k1, v1, k2, v2, ...)
     *
     * @param data keys and values (in pairs)
     * @param <K>  type of keys
     * @param <V>  type of values
     * @return mutable map
     */
    public static <K, V> TreeMap<K, V> tmap(Object... data) {
        TreeMap<K, V> map = new TreeMap<K, V>();

        for (int i = 1; i < data.length; i += 2) {
            map.put((K) data[i - 1], (V) data[i]);
        }

        return map;
    }

    /**
     * Equivalent of map(k1, v1, ...) that returns constant (unmodifiable) map.
     *
     * @param data key value pairs (k1, v1, k2, v2, ...)
     * @param <K>  type of keys
     * @param <V>  type of values
     * @return immutable map
     */
    public static <K, V> Map<K, V> constMap(Object... data) {
        Map<K, V> map = map(data);

        return Collections.unmodifiableMap(map);
    }


    public static String bufToString(byte[] buf) {
        return bufToString(buf, 0, buf.length);
    }


    public static String bufToString(byte[] buf, int offs, int len) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < len; i++) {
            int ch = buf[i+offs] & 0xff;
            if (ch < 32 || ch >= 127) {
                sb.append(String.format("[\\%02x]", ch));
            } else {
                sb.append((char) ch);
            }
        }

        return sb.toString();
    }


    /**
     * Creates a set from supplied strings.
     *
     * @param objs members of newly formed set
     * @return set of strings
     */
    public static <T> Set<T> set(T...objs) {
        Set<T> set = new HashSet<>(objs.length * 2 + 1);
        Collections.addAll(set, objs);

        return set;
    }

    public static <T> Set<T> i2set(Iterable<T> source) {
        Set<T> set = new HashSet<>();
        for (T v : source) {
            set.add(v);
        }
        return set;
    }

    public static <T> Set<T> constSet(T...objs) {
        Set<T> set = set(objs);

        return Collections.unmodifiableSet(set);
    }

    public static Properties props(String...s) {
        Properties props = new Properties();

        for (int i = 1; i < s.length; i += 2) {
            props.setProperty(s[i-1], s[i]);
        }

        return props;
    }

    private static boolean unmapSupported = true;

    public synchronized static void unmapBuffer(Object mbb) {
        if (unmapSupported) {
            try {
                Method cleanerMethod = mbb.getClass().getMethod("cleaner");
                if (cleanerMethod != null) {
                    cleanerMethod.setAccessible(true);
                    Object cleanerObj = cleanerMethod.invoke(mbb);
                    cleanerMethod.setAccessible(false);
                    if (cleanerObj != null) {
                        Method cleanMethod = cleanerObj.getClass().getMethod("clean");
                        if (cleanMethod != null) {
                            cleanMethod.invoke(cleanerObj);
                        }
                    } else {
                        Method attachmentMethod = mbb.getClass().getMethod("attachment");
                        attachmentMethod.setAccessible(true);
                        Object attachmentObj = attachmentMethod.invoke(mbb);
                        attachmentMethod.setAccessible(false);
                        if (attachmentObj instanceof MappedByteBuffer) {
                            unmapBuffer(attachmentObj);
                        }
                    }
                }
            } catch (Exception e) {
                unmapSupported = false;
            }
        }
    }

    public static boolean equals(byte[] a, byte[] b) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null || a.length != b.length) {
            return false;
        }
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i])
                return false;
        }
        return true;
    }

    private final static ThreadLocal<CRC32> crc32 = new ThreadLocal<CRC32>() {
        public CRC32 initialValue() {
            return new CRC32();
        }
    };

    public static long crc32(byte[] buf) {
        return crc32(buf, 0, buf.length);
    }

    public static long crc32(byte[] buf, int offs, int len) {
        CRC32 crc = crc32.get();
        crc.reset();
        crc.update(buf, offs, len);
        return crc.getValue();
    }


    public static void reverse(long[] arr, int from, int to) {
        if (to - from < 2) {
            return;
        }

        int l = (to - from) / 2;

        for (int i = 0; i < l; i++) {
            long tmp = arr[from+i];
            arr[from+i] = arr[to-i-1];
            arr[to-i-1] = tmp;
        }
    }

    public static void reverse(byte[] arr) {
        reverse(arr, 0, arr.length);
    }

    /**
     * Reverses order of bytes in given part of array
     * @param arr byte array
     * @param from start index (inclusive)
     * @param to end index (exclusive)
     */
    public static void reverse(byte[] arr, int from, int to) {
        if (to - from < 2) {
            return;
        }

        int l = (to - from) / 2;

        for (int i = 0; i < l; i++) {
            byte tmp = arr[from+i];
            arr[from+i] = arr[to-i-1];
            arr[to-i-1] = tmp;
        }
    }

    /**
     * Print short class name
     */
    public static final int PS_SHORT_CLASS = 0x01;

    /**
     * Print result type
     */
    public static final int PS_RESULT_TYPE = 0x02;

    /**
     * Print short argument types
     */
    public static final int PS_SHORT_ARGS = 0x04;

    /**
     * Omits arguments overall in pretty pring
     */
    public static final int PS_NO_ARGS = 0x08;


    private static final Pattern RE_DOT = Pattern.compile("\\.");


    public static String shortClassName(String className) {
        String[] segs = RE_DOT.split(className != null ? className : "");
        return segs[segs.length - 1];
    }


    /**
     * Returns human readable method description (with default flags)
     *
     * @return method description string
     */
    public static String prettyPrint(String cname, String mname, String msign) {
        return prettyPrint(cname, mname, msign, PS_RESULT_TYPE | PS_SHORT_ARGS);
    }


    /**
     * Returns human readable method description (configurable with supplied flags)
     *
     * @param style style flags (see PS_* constants)
     * @return method description string
     */
    public static String prettyPrint(String cname, String mname, String msign, int style) {
        StringBuilder sb = new StringBuilder(128);

        // Print return type
        if (0 != (style & PS_RESULT_TYPE)) {
            Type retType = Type.getReturnType(msign);
            if (0 != (style & PS_SHORT_ARGS)) {
                sb.append(shortClassName(retType.getClassName()));
            } else {
                sb.append(retType.getClassName());
            }
            sb.append(" ");
        }

        // Print class name
        if (0 != (style & PS_SHORT_CLASS)) {
            sb.append(shortClassName(cname));
        } else {
            sb.append(cname);
        }

        sb.append(".");
        sb.append(mname);
        sb.append("(");

        // Print arguments (if needed)
        if (0 == (style & PS_NO_ARGS)) {
            Type[] types = Type.getArgumentTypes(msign);
            for (int i = 0; i < types.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                if (0 != (style & PS_SHORT_ARGS)) {
                    sb.append(shortClassName(types[i].getClassName()));
                } else {
                    sb.append(types[i].getClassName());
                }
            }
        }

        sb.append(")");

        return sb.toString();
    }


    /**
     * Tries to coerce a value to a specific (simple) data type.
     *
     * @param val value to be coerced (converted)
     * @param c   destination type class
     * @return coerced value
     */
    public static Object coerce(Object val, Class<?> c) {

        if (val == null || c == null) {
            return null;
        } else if (val.getClass() == c) {
            return val;
        } else if (c == String.class) {
            return castString(val);
        } else if (c == Boolean.class || c == Boolean.TYPE) {
            return coerceBool(val);
        } else if (c == Long.class || c == Long.TYPE) {
            return castLong(val);
        } else if (c == Integer.class || c == Integer.TYPE) {
            return castInteger(val);
        } else if (c == Double.class || c == Double.TYPE) {
            return castDouble(val);
        } else if (c == Short.class || c == Short.TYPE) {
            return castShort(val);
        } else if (c == Float.class || c == Float.TYPE) {
            return castFloat(val);
        }

        return null;
    }

    public static String castString(Object val) {
        try {
            return val != null ? val.toString() : "null";
        } catch (Exception e) {
            return "<ERR: " + e.getMessage() + ">";
        }
    }

    private static float castFloat(Object val) {
        return (val instanceof String)
            ? Float.parseFloat(val.toString().trim())
            : ((Number) val).floatValue();
    }

    private static short castShort(Object val) {
        return (val instanceof String)
            ? Short.parseShort(val.toString().trim())
            : ((Number) val).shortValue();
    }

    private static double castDouble(Object val) {
        return (val instanceof String)
            ? Double.parseDouble(val.toString().trim())
            : ((Number) val).doubleValue();
    }

    private static int castInteger(Object val) {
        return (val instanceof String)
            ? Integer.parseInt(val.toString().trim())
            : ((Number) val).intValue();
    }

    private static long castLong(Object val) {
        return (val instanceof String)
            ? Long.parseLong(val.toString().trim())
            : ((Number) val).longValue();
    }

    /**
     * Coerces any value to boolean.
     *
     * @param val value to be coerced
     * @return false if value is null or boolean false, true otherwise
     */
    public static boolean coerceBool(Object val) {
        return !(val == null || val.equals(false));
    }


    /**
     * Checks if given class instance of given type. Check is done only by name, so if two classes of the same
     * name are loaded by two independent class loaders, they'll appear to be the same. This is enough if
     * object implementing given class is then accessed using reflection. As this method uses only name comparison,
     *
     * @param c    class to be checked
     * @param name class or interface full name (with package prefix)
     * @return true if class c is a or implements type named 'name'
     */
    public static boolean instanceOf(Class<?> c, String name) {

        for (Class<?> clazz = c; clazz != null && !"java.lang.Object".equals(clazz.getName()); clazz = clazz.getSuperclass()) {
            if (name.equals(clazz.getName()) || interfaceOf(clazz, name)) {
                return true;
            }
        }

        return false;
    }


    public static boolean instanceOf(Class<?> c, Pattern clPattern) {

        for (Class<?> clazz = c; clazz != null && !"java.lang.Object".equals(clazz.getName()); clazz = clazz.getSuperclass()) {
            if (clPattern.matcher(clazz.getName()).matches() || interfaceOf(clazz, clPattern)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks if given class implements interface of name given by ifName. Analogous to above instanceOf() method.
     *
     * @param c      class to be tested
     * @param ifName interface full name (with package prefix)
     * @return true if class c implements interface named 'ifName'
     */
    public static boolean interfaceOf(Class<?> c, String ifName) {
        for (Class<?> ifc : c.getInterfaces()) {
            if (ifName.equals(ifc.getName()) || interfaceOf(ifc, ifName)) {
                return true;
            }
        }

        return false;
    }


    public static boolean interfaceOf(Class<?> c, Pattern ifcPattern) {
        for (Class<?> ifc : c.getInterfaces()) {
            if (ifcPattern.matcher(ifc.getName()).matches() || interfaceOf(ifc, ifcPattern)) {
                return true;
            }
        }

        return false;
    }


    /**
     * Returns string that contains of all elements of passed collection joined together
     *
     * @param sep separator (inserted between values)
     * @param col collection of values to concatenate
     * @return concatenated string
     */
    public static String join(String sep, Collection<?> col) {
        StringBuilder sb = new StringBuilder();

        for (Object val : col) {
            if (sb.length() > 0) sb.append(sep);
            sb.append(castString(val));
        }

        return sb.toString();
    }


    /**
     * Returns string that contains of all elements of passed (vararg) array joined together
     *
     * @param sep  separator (inserted between values)
     * @param vals array of values
     * @return concatenated string
     */
    public static String join(String sep, Object... vals) {
        StringBuilder sb = new StringBuilder();

        for (Object val : vals) {
            if (sb.length() > 0) sb.append(sep);
            sb.append(castString(val));
        }

        return sb.toString();
    }

    public static <T> List<T> drain(Iterable<T> src) {
        List<T> lst = new ArrayList<>();
        for (T item : src) {
            lst.add(item);
        }
        return lst;
    }

    public static int nextPow2(int v) {
        for (int i = 1; i < Integer.MAX_VALUE/2; i *= 2) {
            if (i > v) return i;
        }
        return Integer.MAX_VALUE;
    }

}
