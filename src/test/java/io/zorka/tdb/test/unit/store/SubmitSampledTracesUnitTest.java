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

package io.zorka.tdb.test.unit.store;

import io.zorka.tdb.meta.ChunkMetadata;
import io.zorka.tdb.search.QueryBuilder;
import io.zorka.tdb.search.rslt.SearchResult;
import io.zorka.tdb.store.RotatingTraceStore;
import io.zorka.tdb.test.support.ZicoTestFixture;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;


public class SubmitSampledTracesUnitTest extends ZicoTestFixture {

    private final static String[] AGD_PACKETS = {
        "wYMZAoR4LG9yZy5hcGFjaGUuY2F0YWxpbmEuY29yZS5TdGFuZGFyZEVuZ2luZVZhbHZlBcGDGI1maW52b2tlBsGDGQJ5eFIoTG9yZy9hcGFjaGUvY2F0YWxpbmEvY29ubmVjdG9yL1JlcXVlc3Q7TG9yZy9hcGFjaGUvY2F0YWxpbmEvY29ubmVjdG9yL1Jlc3BvbnNlOylWCMKEARkChBiNGQJ5wYMBY1VSSQDBgwNmU1RBVFVTAMGDGQaZa0hkckluX19ob3N0AMGDGQaacUhkckluX19jb25uZWN0aW9uAMGDGQabbUhkckluX19wcmFnbWEAwYMZBpx0SGRySW5fX2NhY2hlLWNvbnRyb2wAwYMZBp14IEhkckluX191cGdyYWRlLWluc2VjdXJlLXJlcXVlc3RzAMGDGQaecUhkckluX191c2VyLWFnZW50AMGDGQafbUhkckluX19hY2NlcHQAwYMZBqBqSGRySW5fX2RudADBgxkGoXZIZHJJbl9fYWNjZXB0LWVuY29kaW5nAMGDGQaidkhkckluX19hY2NlcHQtbGFuZ3VhZ2UAwYMZBIl4LW9yZy5hcGFjaGUuY2F0YWxpbmEuY29yZS5TdGFuZGFyZENvbnRleHRWYWx2ZQXChAIZBIkYjRkCecGDGQZpeDFvcmcuYXBhY2hlLmNhdGFsaW5hLmNvcmUuQXBwbGljYXRpb25GaWx0ZXJGYWN0b3J5BcGDGQZqcWNyZWF0ZUZpbHRlckNoYWluBsGDGQZreIUoTGphdmF4L3NlcnZsZXQvU2VydmxldFJlcXVlc3Q7TG9yZy9hcGFjaGUvY2F0YWxpbmEvV3JhcHBlcjtMamF2YXgvc2VydmxldC9TZXJ2bGV0OylMb3JnL2FwYWNoZS9jYXRhbGluYS9jb3JlL0FwcGxpY2F0aW9uRmlsdGVyQ2hhaW47CMKEAxkGaRkGahkGa8GDGQZseC9vcmcuYXBhY2hlLmNhdGFsaW5hLmNvcmUuQXBwbGljYXRpb25GaWx0ZXJDaGFpbgXBgxkFX2hkb0ZpbHRlcgbBgxkGbXhAKExqYXZheC9zZXJ2bGV0L1NlcnZsZXRSZXF1ZXN0O0xqYXZheC9zZXJ2bGV0L1NlcnZsZXRSZXNwb25zZTspVgjChAQZBmwZBV8ZBm3BgxkFVngvb3JnLmFwYWNoZS5jYXRhbGluYS5jb3JlLkRlZmF1bHRJbnN0YW5jZU1hbmFnZXIFwYMZBVdrbmV3SW5zdGFuY2UGwYMZBVh4PShMamF2YS9sYW5nL1N0cmluZztMamF2YS9sYW5nL0NsYXNzTG9hZGVyOylMamF2YS9sYW5nL09iamVjdDsIwoQFGQVWGQVXGQVYwYMZBoZ4GG9yZy5hcGFjaGUuanNwLmluZGV4X2pzcAXBgxkGh2hfanNwSW5pdAbBgxJjKClWCMKEBhkGhhkGhxLBgxkGiWtfanNwU2VydmljZQbBgxkGinhSKExqYXZheC9zZXJ2bGV0L2h0dHAvSHR0cFNlcnZsZXRSZXF1ZXN0O0xqYXZheC9zZXJ2bGV0L2h0dHAvSHR0cFNlcnZsZXRSZXNwb25zZTspVgjChAcZBoYZBokZBoo="
    };

    private final static String[] TRC_PACKETS = {
        "yJ/KSAAAAQAAAAAA2CGCGwAAAV4I1MQSGCDJrMYBYS/GA2MyMDDGGQaZbmxvY2FsaG9zdDo4MDgwxhkGmmprZWVwLWFsaXZlxhkGm2huby1jYWNoZcYZBpxobm8tY2FjaGXGGQadYTHGGQaeeGlNb3ppbGxhLzUuMCAoWDExOyBMaW51eCB4ODZfNjQpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hUTUwsIGxpa2UgR2Vja28pIENocm9tZS82MC4wLjMxMTIuMTAxIFNhZmFyaS81MzcuMzbGGQafeFV0ZXh0L2h0bWwsYXBwbGljYXRpb24veGh0bWwreG1sLGFwcGxpY2F0aW9uL3htbDtxPTAuOSxpbWFnZS93ZWJwLGltYWdlL2FwbmcsKi8qO3E9MC44xhkGoGExxhkGoXFnemlwLCBkZWZsYXRlLCBicsYZBqJ4IGVuLVVTLGVuO3E9MC44LHBsO3E9MC42LHJ1O3E9MC40yJ/KSAAAAgAAAAAAyJ/KSAAAAwAAAAAAzEgAAAIAAAAAsf/In8pIAAAEAACxHPvIn8pIAAAFAACxHPvMSAAAAQAAsR08/8ifykgAAAYAAPLqR8xIAAABAADy6rP/yJ/KSAAABwABX4KCzEgAAAEAAV+Eq//MSAAABAAAsSBc/8xIAAALAAAABFL/zEgAAAwAAAAFWv8="
    };

    private ChunkMetadata cm(int envId, int appId) {
        ChunkMetadata rslt = new ChunkMetadata();
        rslt.setEnvId(envId);
        rslt.setAppId(appId);
        return rslt;
    }

    @Test @Ignore("Fix me.")
    public void testSubmitTomcatOldTracerSinglePkt() throws Exception {
        RotatingTraceStore store = openRotatingStore();

        String agentUUID = UUID.randomUUID().toString();
        String sessnUUID = store.getSession(agentUUID);

        String trc01UUID = UUID.randomUUID().toString();

        // TODO fix input data, so trace ID is proper

        store.handleAgentData(agentUUID, sessnUUID, AGD_PACKETS[0]);
        store.handleTraceData(agentUUID, sessnUUID, trc01UUID, TRC_PACKETS[0], cm(1, 1));

        SearchResult sr0 = store.search(QueryBuilder.qmi().node());
        Set<Long> sl0 = drain(sr0);
        assertEquals("Should store exactly one record.", 1, sl0.size());
    }

}
