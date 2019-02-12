/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2019 IBM.
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
package org.onap.music.rest.service;

import javax.ws.rs.core.Response;

import org.onap.music.datastore.jsonobjects.CassaKeyspaceObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.springframework.util.MultiValueMap;

public interface MusicDataAPIService {

    /**
     * For Creating KeySpace.
     * 
     * @param version
     * @param minorVersion
     * @param patchVersion
     * @param authorization
     * @param aid
     * @param ns
     * @param kspObject
     * @param keyspaceName
     * @return
     */
    public Response createKeySpace(String version, String minorVersion, String patchVersion, String authorization,
            String aid, String ns, CassaKeyspaceObject kspObject, String keyspaceName);

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
    public Response dropKeySpace(String version, String minorVersion, String patchVersion, String authorization,
            String aid, String ns, CassaKeyspaceObject kspObject, String keyspaceName);

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
            String aid, String ns, JsonTable tableObj, String keyspace, String tablename);

    /**
     * For Creating Index
     * 
     * @param version
     * @param minorVersion
     * @param patchVersion
     * @param aid
     * @param ns
     * @param authorization
     * @param keyspace
     * @param tablename
     * @param fieldName
     * @return
     */
    public Response createIndex(String version, String minorVersion, String patchVersion, String aid, String ns,
            String authorization, String keyspace, String tablename, String fieldName, MultiValueMap<String, String> requestParam);

    /**
     * For inserting Record into table
     * 
     * @param version
     * @param minorVersion
     * @param patchVersion
     * @param aid
     * @param ns
     * @param authorization
     * @param insObj
     * @param keyspace
     * @param tablename
     * @return
     */
    public Response insertIntoTable(String version, String minorVersion, String patchVersion, String aid, String ns,
            String authorization, JsonInsert insObj, String keyspace, String tablename);

    /**
     * For Update Table
     * 
     * @param version
     * @param minorVersion
     * @param patchVersion
     * @param aid
     * @param ns
     * @param authorization
     * @param updateObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     */
    public Response updateTable(String version, String minorVersion, String patchVersion, String aid, String ns,
            String authorization, JsonUpdate updateObj, String keyspace, String tablename, MultiValueMap<String, String> requestParam);

    /**
     * For delete from table
     * 
     * @param version
     * @param minorVersion
     * @param patchVersion
     * @param aid
     * @param ns
     * @param authorization
     * @param delObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     */
    public Response deleteFromTable(String version, String minorVersion, String patchVersion, String aid, String ns,
            String authorization, JsonDelete delObj, String keyspace, String tablename, MultiValueMap<String, String> requestParam);

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
            String authorization, String keyspace, String tablename);

    /**
     * For getting Critical
     * 
     * @param version
     * @param minorVersion
     * @param patchVersion
     * @param aid
     * @param ns
     * @param authorization
     * @param selObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     */
    public Response selectCritical(String version, String minorVersion, String patchVersion, String aid, String ns,
            String authorization, JsonInsert selObj, String keyspace, String tablename, MultiValueMap<String, String> requestParam);

    /**
     * For Select Rows
     * 
     * @param version
     * @param minorVersion
     * @param patchVersion
     * @param aid
     * @param ns
     * @param authorization
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     */
    public Response select(String version, String minorVersion, String patchVersion, String aid, String ns,
            String authorization, String keyspace, String tablename, MultiValueMap<String, String> requestParam);

}
