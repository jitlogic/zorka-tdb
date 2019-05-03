/*
 * Copyright 2016-2019 Rafal Lewczuk <rafal.lewczuk@jitlogic.com>
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
import io.zorka.tdb.test.support.ZicoTestFixture;
import io.zorka.tdb.search.QmiQueryBuilder;
import io.zorka.tdb.store.RotatingTraceStore;
import io.zorka.tdb.store.TraceSearchResult;
import org.junit.Ignore;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;


public class SubmitSampledTracesUnitTest extends ZicoTestFixture {

    private final static byte[][] AGD_PACKETS = {
            DatatypeConverter.parseBase64Binary("wYMZAoR4LG9yZy5hcGFjaGUuY2F0YWxpbmEuY29yZS5TdGFuZGFyZEVuZ2luZVZhbHZlBcGDGI1maW52b2tlBsGDGQJ5eFIoTG9yZy9hcGFjaGUvY2F0YWxpbmEvY29ubmVjdG9yL1JlcXVlc3Q7TG9yZy9hcGFjaGUvY2F0YWxpbmEvY29ubmVjdG9yL1Jlc3BvbnNlOylWCMKEARkChBiNGQJ5wYMBY1VSSQDBgwNmU1RBVFVTAMGDGQaZa0hkckluX19ob3N0AMGDGQaacUhkckluX19jb25uZWN0aW9uAMGDGQabbUhkckluX19wcmFnbWEAwYMZBpx0SGRySW5fX2NhY2hlLWNvbnRyb2wAwYMZBp14IEhkckluX191cGdyYWRlLWluc2VjdXJlLXJlcXVlc3RzAMGDGQaecUhkckluX191c2VyLWFnZW50AMGDGQafbUhkckluX19hY2NlcHQAwYMZBqBqSGRySW5fX2RudADBgxkGoXZIZHJJbl9fYWNjZXB0LWVuY29kaW5nAMGDGQaidkhkckluX19hY2NlcHQtbGFuZ3VhZ2UAwYMZBIl4LW9yZy5hcGFjaGUuY2F0YWxpbmEuY29yZS5TdGFuZGFyZENvbnRleHRWYWx2ZQXChAIZBIkYjRkCecGDGQZpeDFvcmcuYXBhY2hlLmNhdGFsaW5hLmNvcmUuQXBwbGljYXRpb25GaWx0ZXJGYWN0b3J5BcGDGQZqcWNyZWF0ZUZpbHRlckNoYWluBsGDGQZreIUoTGphdmF4L3NlcnZsZXQvU2VydmxldFJlcXVlc3Q7TG9yZy9hcGFjaGUvY2F0YWxpbmEvV3JhcHBlcjtMamF2YXgvc2VydmxldC9TZXJ2bGV0OylMb3JnL2FwYWNoZS9jYXRhbGluYS9jb3JlL0FwcGxpY2F0aW9uRmlsdGVyQ2hhaW47CMKEAxkGaRkGahkGa8GDGQZseC9vcmcuYXBhY2hlLmNhdGFsaW5hLmNvcmUuQXBwbGljYXRpb25GaWx0ZXJDaGFpbgXBgxkFX2hkb0ZpbHRlcgbBgxkGbXhAKExqYXZheC9zZXJ2bGV0L1NlcnZsZXRSZXF1ZXN0O0xqYXZheC9zZXJ2bGV0L1NlcnZsZXRSZXNwb25zZTspVgjChAQZBmwZBV8ZBm3BgxkFVngvb3JnLmFwYWNoZS5jYXRhbGluYS5jb3JlLkRlZmF1bHRJbnN0YW5jZU1hbmFnZXIFwYMZBVdrbmV3SW5zdGFuY2UGwYMZBVh4PShMamF2YS9sYW5nL1N0cmluZztMamF2YS9sYW5nL0NsYXNzTG9hZGVyOylMamF2YS9sYW5nL09iamVjdDsIwoQFGQVWGQVXGQVYwYMZBoZ4GG9yZy5hcGFjaGUuanNwLmluZGV4X2pzcAXBgxkGh2hfanNwSW5pdAbBgxJjKClWCMKEBhkGhhkGhxLBgxkGiWtfanNwU2VydmljZQbBgxkGinhSKExqYXZheC9zZXJ2bGV0L2h0dHAvSHR0cFNlcnZsZXRSZXF1ZXN0O0xqYXZheC9zZXJ2bGV0L2h0dHAvSHR0cFNlcnZsZXRSZXNwb25zZTspVgjChAcZBoYZBokZBoo=")
    };

    private final static byte[][] TRC_PACKETS = {
        DatatypeConverter.parseBase64Binary("yJ/KSAAAAQAAAAAA2CGCGwAAAV4I1MQSGCDJrMYBYS/GA2MyMDDGGQaZbmxvY2FsaG9zdDo4MDgwxhkGmmprZWVwLWFsaXZlxhkGm2huby1jYWNoZcYZBpxobm8tY2FjaGXGGQadYTHGGQaeeGlNb3ppbGxhLzUuMCAoWDExOyBMaW51eCB4ODZfNjQpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hUTUwsIGxpa2UgR2Vja28pIENocm9tZS82MC4wLjMxMTIuMTAxIFNhZmFyaS81MzcuMzbGGQafeFV0ZXh0L2h0bWwsYXBwbGljYXRpb24veGh0bWwreG1sLGFwcGxpY2F0aW9uL3htbDtxPTAuOSxpbWFnZS93ZWJwLGltYWdlL2FwbmcsKi8qO3E9MC44xhkGoGExxhkGoXFnemlwLCBkZWZsYXRlLCBicsYZBqJ4IGVuLVVTLGVuO3E9MC44LHBsO3E9MC42LHJ1O3E9MC40yJ/KSAAAAgAAAAAAyJ/KSAAAAwAAAAAAzEgAAAIAAAAAsf/In8pIAAAEAACxHPvIn8pIAAAFAACxHPvMSAAAAQAAsR08/8ifykgAAAYAAPLqR8xIAAABAADy6rP/yJ/KSAAABwABX4KCzEgAAAEAAV+Eq//MSAAABAAAsSBc/8xIAAALAAAABFL/zEgAAAwAAAAFWv8=")
    };

    private ChunkMetadata cm() {
        return new ChunkMetadata(rand.nextLong(), rand.nextLong(), 0L, 1L, 0);
    }

    @Test @Ignore("Fix me.")
    public void testSubmitTomcatOldTracerSinglePkt() throws Exception {
        RotatingTraceStore store = openRotatingStore();

        String sessnUUID = UUID.randomUUID().toString();

        String trc01UUID = UUID.randomUUID().toString();

        // TODO fix input data, so trace ID is proper

        store.handleAgentData(sessnUUID, false, AGD_PACKETS[0]);
        store.handleTraceData(sessnUUID, TRC_PACKETS[0], cm());

        TraceSearchResult sr0 = store.searchTraces(QmiQueryBuilder.all().query());
        Set sl0 = drain(sr0);
        assertEquals("Should store exactly one record.", 1, sl0.size());
    }

}
