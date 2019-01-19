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

package io.zorka.tdb;

/**
 * Thrown when agent tries to use invalid session ID.
 */
public class MissingSessionException extends ZicoException {

    private String sessionUUID, agentUUID;

    public MissingSessionException(String sessionUUID, String agentUUID) {
        super("Invalid session UUID: " + sessionUUID + " for agent " + agentUUID);
        this.sessionUUID = sessionUUID;
        this.agentUUID = agentUUID;
    }

    public String getSessionUUID() {
        return sessionUUID;
    }

    public String getAgentUUID() {
        return agentUUID;
    }
}
