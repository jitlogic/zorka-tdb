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

import io.zorka.tdb.util.JSONReader;
import io.zorka.tdb.util.ZicoUtil;
import io.zorka.tdb.util.ZicoUtil;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class TestGenerator {
    private Map<String,List<Object>> strParts = new HashMap<>();
    private Random rand = new Random();

    public TestGenerator() {
        try {
            readParts();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void readParts() throws Exception {
        String json = new String(TestUtil.readc("/testdata/text_gen.json", false));
        Map<Object,Object> map = (Map<Object,Object>)new JSONReader().read(json);
        for (Map.Entry<Object,Object> e : map.entrySet()) {
            strParts.put((String)e.getKey(), (List<Object>)e.getValue());
        }
    }

    private String genPart(String label) {
        return strParts.get(label).get(rand.nextInt(strParts.get(label).size())).toString();
    }


    public ConcurrentNavigableMap<String,Integer> genDictionary(int nstrings) {
        ConcurrentNavigableMap<String,Integer> rslt = new ConcurrentSkipListMap<>();
        int nextId = 1;

        for (int i = 0; i < nstrings; i++) {
            switch (rand.nextInt(3)) {
                case 0: {
                    // Generate SQL expression
                    rslt.putIfAbsent(genPart("SQL_SELECT") + " " + genPart("SQL_FIELDS") + " " + genPart("SQL_FROM") + " "
                        + genPart("SQL_TABLE") + " " + genPart("SQL_WHERE") + " " + genPart("SQL_CLAUSES"), nextId++);
                    break;
                }
                case 1: {
                    // Generate timestamp (from last 30 days)
                    rslt.putIfAbsent(new Date(System.currentTimeMillis() - (rand.nextLong() % 2592000000L)).toString(), nextId++);
                    break;
                }
                case 2: {
                    // Generate sample session ID
                    byte[] b = new byte[32];
                    rand.nextBytes(b);
                    rslt.putIfAbsent(ZicoUtil.hex(b), nextId++);
                    break;
                }
            }
        }

        return rslt;
    }

}
