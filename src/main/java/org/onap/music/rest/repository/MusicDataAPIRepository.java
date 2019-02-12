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
package org.onap.music.rest.repository;

import java.util.Map;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.onap.music.datastore.jsonobjects.CassaKeyspaceObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.ResultType;
import org.springframework.util.MultiValueMap;

public interface MusicDataAPIRepository {

    /**
     * Method to authenticate to Music
     * 
     * @param ns
     * @param userId
     * @param password
     * @param keyspaceName
     * @param aid
     * @return
     * @throws Exception
     */
    public Map<String, Object> musicAuthentication(String ns, String userId, String password, String keyspaceName,
            String aid, String operationType) throws Exception;

    /**
     * 
     * @param kspObject
     * @param keyspaceName
     * @param consistency
     * @return
     * @throws MusicServiceException
     */
    public ResultType createKeySpace(CassaKeyspaceObject cassaKeyspaceObject) throws MusicServiceException;

    /**
     * 
     * @param userId
     * @param password
     * @param keyspaceName
     * @param consistency
     * @return
     */
    public ResultType createRole(String userId, String password, String keyspaceName, String consistency)
            throws MusicServiceException;

    /**
     * 
     * @param userId
     * @param password
     * @param ns
     * @param newAid
     * @param keyspaceName
     * @throws Exception
     */
    public void createKeySpaceMasterEntry(String userId, String password, String ns, String newAid, String keyspaceName)
            throws Exception;

    /**
     * 
     * @param keyspaceName
     * @return
     * @throws MusicServiceException
     */
    public long findKeySpaceMasterResultCountByKeySpaceName(String keyspaceName) throws Exception;

    /**
     * 
     * @param keyspaceName
     * @param consistency
     * @throws Exception
     */
    public void updateKeyspaceMaster(String keyspaceName, String consistency) throws Exception;

    /**
     * 
     * @param keyspaceName
     * @param consistency
     * @throws Exception
     */
    public void deleteKeyspaceMaster(String keyspaceName, String consistency) throws Exception;

    /**
     * 
     * @param keyspaceName
     * @param consistency
     * @return
     * @throws MusicServiceException
     */
    public ResultType dropKeyspace(CassaKeyspaceObject cassaKeyspaceObject) throws MusicServiceException;

    /**
     * 
     * @param response
     * @param tableObj
     * @param keyspace
     * @param tablename
     * @return
     */
    public Response createTable(ResponseBuilder response, JsonTable tableObj, String keyspace, String tablename);

    /**
     * 
     * @param response
     * @param fieldName
     * @param keyspace
     * @param tablename
     * @param requestParam
     * @return
     */
    public Response createIndex(ResponseBuilder response, String fieldName, String keyspace, String tablename,
            MultiValueMap<String, String> requestParam);

    /**
     * 
     * @param response
     * @param insObj
     * @param keyspace
     * @param tablename
     * @return
     */
    public Response insertIntoTable(ResponseBuilder response, JsonInsert insObj, String keyspace, String tablename);

    /**
     * 
     * @param response
     * @param updateObj
     * @param keyspace
     * @param tablename
     * @param requestParam
     * @return
     */
    public Response updateTable(ResponseBuilder response, JsonUpdate updateObj, String keyspace, String tablename,
            MultiValueMap<String, String> requestParam);

    /**
     * 
     * @param response
     * @param delObj
     * @param keyspace
     * @param tablename
     * @param requestParam
     * @return
     */
    public Response deleteFromTable(ResponseBuilder response, JsonDelete delObj, String keyspace, String tablename,
            MultiValueMap<String, String> requestParam);

    /**
     * 
     * @param response
     * @param keyspace
     * @param tablename
     * @return
     */
    public Response dropTable(ResponseBuilder response, String keyspace, String tablename);

    /**
     * 
     * @param response
     * @param selObj
     * @param keyspace
     * @param tablename
     * @param requestParam
     * @return
     */
    public Response selectCritical(ResponseBuilder response, JsonInsert selObj, String keyspace, String tablename,
            MultiValueMap<String, String> requestParam);

    /**
     * 
     * @param response
     * @param keyspace
     * @param tablename
     * @param requestParam
     * @return
     */
    public Response select(ResponseBuilder response, String version, String minorVersion, String patchVersion,
            String aid, String ns, String userId, String password, String keyspace, String tablename,
            MultiValueMap<String, String> requestParam);
}
