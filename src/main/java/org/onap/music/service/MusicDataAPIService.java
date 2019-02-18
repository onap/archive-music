/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 * Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 * Copyright (c) 2019 IBM.
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
package org.onap.music.service;

import javax.ws.rs.core.Response;

import org.onap.music.datastore.jsonobjects.CassaKeyspaceObject;
import org.onap.music.datastore.jsonobjects.CassaTableObject;

public interface MusicDataAPIService {

    /**
     * For Creating Keyspace
     * 
     * @param version
     * @param keyspace_active
     * @param minorVersion
     * @param patchVersion
     * @param authorization
     * @param aid
     * @param ns
     * @param kspObject
     * @param keyspaceName
     * @return
     */
    public Response createKeySpace(String version, boolean keyspace_active, String minorVersion, String patchVersion,
            String authorization, String aid, String ns, CassaKeyspaceObject kspObject, String keyspaceName);

    /**
     * For Deleting KeySpace.
     * 
     * @param version
     * @param minorVersion
     * @param patchVersion
     * @param authorization
     * @param aid
     * @param ns
     * @param keyspaceName
     * @return
     */
    public Response dropKeySpace(String version, boolean keyspace_active, String minorVersion, String patchVersion, String authorization,
            String aid, String ns, CassaKeyspaceObject kspObject, String keyspaceName) throws Exception;

    /**
     * For Creating a Table
     * 
     * @param version
     * @param minorVersion
     * @param patchVersion
     * @param authorization
     * @param aid
     * @param ns
     * @param keyspace
     * @param tablename
     * @return
     */
    public Response createTable(String version, String minorVersion, String patchVersion, String authorization,
            String aid, String ns, CassaTableObject tableObj, String keyspace, String tablename)throws Exception;

    /**
     * For Drop Table
     * 
     * @param version
     * @param minorVersion
     * @param patchVersion
     * @param aid
     * @param ns
     * @param authorization
     * @param keyspace
     * @param tablename
     * @return
     */
    public Response dropTable(String version, String minorVersion, String patchVersion, String aid, String ns,
            String authorization, String keyspace, String tablename) throws Exception;

}
