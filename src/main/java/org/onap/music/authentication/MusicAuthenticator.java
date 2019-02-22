/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2019 AT&T Intellectual Property
 * ===================================================================
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * 
 * ============LICENSE_END=============================================
 * ====================================================================
 */

package org.onap.music.authentication;

public interface MusicAuthenticator {
    public enum Operation{
        CREATE_KEYSPACE,
        DROP_KEYSPACE,
        CREATE_TABLE,
        CREATE_INDEX,
        INSERT_INTO_TABLE,
        UPDATE_TABLE,
        DELETE_FROM_TABLE,
        DROP_TABLE,
        SELECT_CRITICAL,
        SELECT
    }
    
    /**
     * Authenticate a user account
     * @param namespace - user's namespace
     * @param authorization - basicAuth representation of username/password
     * @param keyspace - keyspace user is trying to access
     * @param aid - aid that identifies the user
     * @param operation - operation that user is trying to do
     * @return true if user has access
     */
    public boolean authenticateUser(String namespace, String authorization,
            String keyspace, String aid, Operation operation);
    
    /**
     * Authenticate an administrative account
     * @param authorization - basicAuth representation of username/password
     * @return true if user has admin privileges
     */
    public boolean authenticateAdmin(String authorization);
    
}
