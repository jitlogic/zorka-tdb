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

package io.zorka.tdb.store;

import io.zorka.tdb.ZicoException;
import io.zorka.tdb.util.CborBufReader;
import io.zorka.tdb.meta.StructuredTextIndex;

import static com.jitlogic.zorka.cbor.TraceDataTags.*;

import static com.jitlogic.zorka.cbor.CBOR.*;


/**
 *
 */
public class AgentDataReader implements Runnable {

    private CborBufReader reader;
    private AgentDataProcessor output;
    private String agentUUID;

    public AgentDataReader(String agentUUID, CborBufReader reader, AgentDataProcessor output) {
        this.reader = reader;
        this.output = output;
        this.agentUUID = agentUUID;
    }


    private void checked(boolean cond, String msg) {
        if (!cond) {
            throw new ZicoException("At pos=" + reader.position() + ": " + msg);
        }
    }

    @Override
    public void run() {
        while (reader.size() - reader.position() > 0) {
            process();
        }
    }

    private void process() {
        int peek = reader.peek(), type = peek & TYPE_MASK;

        checked(type == TAG_BASE, "Expected tagged data.");

        int tag = reader.readInt();

        checked(reader.peekType() == ARR_BASE, "Expected tuple (array type).");

        switch (tag) {
            case TAG_STRING_DEF: {
                int len = reader.readInt();
                checked(len == 3, "Expected 3-item tuple.");
                int remoteId = reader.readInt();
                String s = reader.readStr();
                int t = reader.readInt();
                checked(t == StructuredTextIndex.STRING_TYPE || (t >= StructuredTextIndex.TYPE_MIN && t <= StructuredTextIndex.TYPE_MAX), "Invalid type code:" + t);
                output.defStringRef(remoteId, s, (byte)t);
                break;
            }
            case TAG_METHOD_DEF: {
                int len = reader.readInt();
                checked(len == 4, "Expected 4-item tuple.");
                int remoteId = reader.readInt();
                int classId = reader.readInt();
                int methodId = reader.readInt();
                int signatureId = reader.readInt();
                output.defMethodRef(remoteId, classId, methodId, signatureId);
                break;
            }
            case TAG_AGENT_ATTR: {
                int len = reader.readInt();
                checked(len == 2, "Expected 2-item tuple.");
                String key = reader.readStr();
                String val = reader.readStr();
                output.defAgentAttr(agentUUID, key, val);
                break;
            }
            default:
                throw new ZicoException("Invalid tag: " + tag);
        }
    }
}
