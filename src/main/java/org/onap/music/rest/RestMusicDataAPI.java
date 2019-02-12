/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Modifications Copyright (c) 2019 IBM
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
package org.onap.music.rest;

import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.onap.music.datastore.jsonobjects.CassaKeyspaceObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.main.MusicUtil;
import org.onap.music.rest.service.MusicDataAPIService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@RestController
@RequestMapping(value = "/rest/v{version:[0-9]+}/keyspaces")
@Api(value = "Data Api")
public class RestMusicDataAPI {
    /*
     * Header values for Versioning X-minorVersion *** - Used to request or
     * communicate a MINOR version back from the client to the server, and from the
     * server back to the client - This will be the MINOR version requested by the
     * client, or the MINOR version of the last MAJOR version (if not specified by
     * the client on the request) - Contains a single position value (e.g. if the
     * full version is 1.24.5, X-minorVersion = "24") - Is optional for the client
     * on request; however, this header should be provided if the client needs to
     * take advantage of MINOR incremented version functionality - Is mandatory for
     * the server on response
     * 
     *** X-patchVersion *** - Used only to communicate a PATCH version in a response
     * for troubleshooting purposes only, and will not be provided by the client on
     * request - This will be the latest PATCH version of the MINOR requested by the
     * client, or the latest PATCH version of the MAJOR (if not specified by the
     * client on the request) - Contains a single position value (e.g. if the full
     * version is 1.24.5, X-patchVersion = "5") - Is mandatory for the server on
     * response (CURRENTLY NOT USED)
     *
     *** X-latestVersion *** - Used only to communicate an API's latest version - Is
     * mandatory for the server on response, and shall include the entire version of
     * the API (e.g. if the full version is 1.24.5, X-latestVersion = "1.24.5") -
     * Used in the response to inform clients that they are not using the latest
     * version of the API (CURRENTLY NOT USED)
     *
     */

    @Autowired
    MusicDataAPIService musicDataAPIService;

    /**
     * This is to Create Keyspace
     * 
     * @param kspObject
     * @param keyspaceName
     * @return
     * @throws Exception
     */
    @PostMapping(value = "/{name}", produces = MediaType.APPLICATION_JSON, consumes = MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create Keyspace", response = String.class)
    public Response createKeySpace(
            @ApiParam(value = "Major Version", required = true) @PathVariable("version") String version,
            @ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = true) @RequestHeader("aid") String aid,
            @ApiParam(value = "Authorization", required = true) @RequestHeader(MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "Application namespace", required = true) @RequestHeader(NS) String ns,
            @RequestBody CassaKeyspaceObject kspObject,
            @ApiParam(value = "Keyspace Name", required = true) @PathVariable("name") String keyspaceName) {

        if (logger.isDebugEnabled()) {
            logger.debug("Comes inside RestMusicDataAPI createKeySpace controller Version  :: " + version);
            logger.debug("Comes inside RestMusicDataAPI createKeySpace controller KeySpace Name :: " + keyspaceName);
            logger.debug("kspspaceObject :: >>>>>>" + kspObject.toString());
        }

        return musicDataAPIService.createKeySpace(VERSION, minorVersion, patchVersion, authorization, aid, ns,
                kspObject, keyspaceName);

    }

    /**
     * This is to delete Keyspace
     * 
     * @param kspObject
     * @param keyspaceName
     * @return
     * @throws Exception
     */
    @DeleteMapping(value = "/delete/{name}", produces = MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Delete Keyspace", response = String.class)
    public Response dropKeySpace(
            @ApiParam(value = "Major Version", required = true) @PathVariable("version") String version,
            @ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = true) @RequestHeader("aid") String aid,
            @ApiParam(value = "Authorization", required = true) @RequestHeader(MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "Application namespace", required = true) @RequestHeader(NS) String ns,
            @RequestBody CassaKeyspaceObject kspObject,
            @ApiParam(value = "Keyspace Name", required = true) @PathVariable("name") String keyspaceName)
            throws Exception {

        if (logger.isDebugEnabled()) {
            logger.debug("Comes inside RestMusicDataAPI createKeySpace controller Version  :: " + version);
            logger.debug("Comes inside RestMusicDataAPI dropKeySpace controller KeySpace Name :: " + keyspaceName);
        }

        return musicDataAPIService.dropKeySpace(VERSION, minorVersion, patchVersion, authorization, aid, ns, kspObject,
                keyspaceName);
    }

    /**
     * This is to Create table
     * 
     * @param tableObj
     * @param version
     * @param keyspace
     * @param tablename
     * @param headers
     * @return
     * @throws Exception
     */
    @PostMapping(value = "/{keyspace}/tables/{tablename}", produces = MediaType.APPLICATION_JSON, consumes = MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create Table", response = String.class)
    public Response createTable(
            @ApiParam(value = "Major Version", required = true) @PathVariable("version") String version,
            @ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = true) @RequestHeader("aid") String aid,
            @ApiParam(value = "Application namespace", required = true) @RequestHeader(NS) String ns,
            @ApiParam(value = "Authorization", required = true) @RequestHeader(MusicUtil.AUTHORIZATION) String authorization,
            @RequestBody JsonTable tableObj,
            @ApiParam(value = "Keyspace Name", required = true) @PathVariable("keyspace") String keyspace,
            @ApiParam(value = "Table Name", required = true) @PathVariable("tablename") String tablename)
            throws Exception {

        if (logger.isDebugEnabled()) {
            logger.debug("Comes inside RestMusicDataAPI createKeySpace controller Version  :: " + version);
            logger.debug("Comes inside RestMusicDataAPI createTable controller KeySpace Name :: " + keyspace
                    + "tableName ::" + tablename);
        }

        return musicDataAPIService.createTable(VERSION, minorVersion, patchVersion, authorization, aid, ns, tableObj,
                keyspace, tablename);
    }

    /**
     * This is to Create Index
     * 
     * @param keyspace
     * @param tablename
     * @param fieldName
     * @param info
     * @throws Exception
     */
    @PostMapping(value = "/{keyspace}/tables/{tablename}/index/{field}", produces = MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create Index", response = String.class)
    public Response createIndex(
            @ApiParam(value = "Major Version", required = true) @PathVariable("version") String version,
            @ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = true) @RequestHeader("aid") String aid,
            @ApiParam(value = "Application namespace", required = true) @RequestHeader(NS) String ns,
            @ApiParam(value = "Authorization", required = true) @RequestHeader(MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "Keyspace Name", required = true) @PathVariable("keyspace") String keyspace,
            @ApiParam(value = "Table Name", required = true) @PathVariable("tablename") String tablename,
            @ApiParam(value = "Field Name", required = true) @PathVariable("field") String fieldName,
            @RequestParam MultiValueMap<String, String> requestParam) throws Exception {

        if (logger.isDebugEnabled()) {
            logger.debug("Comes inside RestMusicDataAPI createKeySpace controller Version  :: " + version);
            logger.debug("Comes inside RestMusicDataAPI createIndex controller KeySpace Name :: " + keyspace
                    + "tableName ::" + tablename + " fieldName ::" + fieldName);
        }

        return musicDataAPIService.createIndex(VERSION, minorVersion, patchVersion, aid, ns, authorization, keyspace,
                tablename, fieldName, requestParam);
    }

    /**
     * This is to insert into Table
     * 
     * @param insObj
     * @param keyspace
     * @param tablename
     * @return
     * @throws Exception
     */
    @PostMapping(value = "/{keyspace}/tables/{tablename}/rows", produces = MediaType.APPLICATION_JSON, consumes = MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Insert Into Table", response = String.class)
    public Response insertIntoTable(
            @ApiParam(value = "Major Version", required = true) @PathVariable("version") String version,
            @ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = true) @RequestHeader("aid") String aid,
            @ApiParam(value = "Application namespace", required = true) @RequestHeader(NS) String ns,
            @ApiParam(value = "Authorization", required = true) @RequestHeader(MusicUtil.AUTHORIZATION) String authorization,
            @RequestBody JsonInsert insObj,
            @ApiParam(value = "Keyspace Name", required = true) @PathVariable("keyspace") String keyspace,
            @ApiParam(value = "Table Name", required = true) @PathVariable("tablename") String tablename) {

        if (logger.isDebugEnabled()) {
            logger.debug("Comes inside RestMusicDataAPI createKeySpace controller Version  :: " + version);
            logger.debug("Comes inside RestMusicDataAPI insertIntoTable controller KeySpace Name :: " + keyspace
                    + "tableName ::" + tablename);
        }

        return musicDataAPIService.insertIntoTable(VERSION, minorVersion, patchVersion, aid, ns, authorization, insObj,
                keyspace, tablename);
    }

    /**
     * This is to update table
     * 
     * @param insObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     * @throws Exception
     */
    @PutMapping(value = "/{keyspace}/tables/{tablename}/rows", produces = MediaType.APPLICATION_JSON, consumes = MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update Table", response = String.class)
    public Response updateTable(
            @ApiParam(value = "Major Version", required = true) @PathVariable("version") String version,
            @ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = true) @RequestHeader("aid") String aid,
            @ApiParam(value = "Application namespace", required = true) @RequestHeader(NS) String ns,
            @ApiParam(value = "Authorization", required = true) @RequestHeader(MusicUtil.AUTHORIZATION) String authorization,
            @RequestBody JsonUpdate updateObj,
            @ApiParam(value = "Keyspace Name", required = true) @PathVariable("keyspace") String keyspace,
            @ApiParam(value = "Table Name", required = true) @PathVariable("tablename") String tablename,
            @RequestParam MultiValueMap<String, String> requestParam) {

        if (logger.isDebugEnabled()) {
            logger.debug("Comes inside RestMusicDataAPI createKeySpace controller Version  :: " + version);
            logger.debug("Comes inside RestMusicDataAPI updateTable controller KeySpace Name :: " + keyspace
                    + "tableName ::" + tablename);
        }

        return musicDataAPIService.updateTable(version, minorVersion, patchVersion, aid, ns, authorization, updateObj,
                keyspace, tablename, requestParam);
    }

    /**
     * This is to delete from table
     * 
     * @param delObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     * @throws Exception
     */
    @DeleteMapping(value = "/{keyspace}/tables/{tablename}/rows", produces = MediaType.APPLICATION_JSON, consumes = MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Delete From table", response = String.class)
    public Response deleteFromTable(
            @ApiParam(value = "Major Version", required = true) @PathVariable("version") String version,
            @ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = true) @RequestHeader("aid") String aid,
            @ApiParam(value = "Application namespace", required = true) @RequestHeader(NS) String ns,
            @ApiParam(value = "Authorization", required = true) @RequestHeader(MusicUtil.AUTHORIZATION) String authorization,
            @RequestBody JsonDelete delObj,
            @ApiParam(value = "Keyspace Name", required = true) @PathVariable("keyspace") String keyspace,
            @ApiParam(value = "Table Name", required = true) @PathVariable("tablename") String tablename,
            @RequestParam MultiValueMap<String, String> requestParam) {

        if (logger.isDebugEnabled()) {
            logger.debug("Comes inside RestMusicDataAPI createKeySpace controller Version  :: " + version);
            logger.debug("Comes inside RestMusicDataAPI deleteFromTable controller KeySpace Name :: " + keyspace
                    + "tableName ::" + tablename);
        }

        return musicDataAPIService.deleteFromTable(VERSION, minorVersion, patchVersion, aid, ns, authorization, delObj,
                keyspace, tablename, requestParam);
    }

    /**
     * This is to drop table
     * 
     * @param tabObj
     * @param keyspace
     * @param tablename
     * @throws Exception
     */
    @DeleteMapping(value = "/{keyspace}/tables/{tablename}", produces = MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Drop Table", response = String.class)
    public Response dropTable(
            @ApiParam(value = "Major Version", required = true) @PathVariable("version") String version,
            @ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = true) @RequestHeader("aid") String aid,
            @ApiParam(value = "Application namespace", required = true) @RequestHeader(NS) String ns,
            @ApiParam(value = "Authorization", required = true) @RequestHeader(MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "Keyspace Name", required = true) @PathVariable("keyspace") String keyspace,
            @ApiParam(value = "Table Name", required = true) @PathVariable("tablename") String tablename)
            throws Exception {

        if (logger.isDebugEnabled()) {
            logger.debug("Comes inside RestMusicDataAPI createKeySpace controller Version  :: " + version);
            logger.debug("Comes inside RestMusicDataAPI dropTable controller KeySpace Name :: " + keyspace
                    + "tableName ::" + tablename);
        }

        return musicDataAPIService.dropTable(VERSION, minorVersion, patchVersion, aid, ns, authorization, keyspace,
                tablename);
    }

    /**
     * This is to get Critical
     * 
     * @param selObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     */
    @PutMapping(value = "/{keyspace}/tables/{tablename}/rows/criticalget", produces = MediaType.APPLICATION_JSON, consumes = MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Select Critical", response = Map.class)
    public Response selectCritical(
            @ApiParam(value = "Major Version", required = true) @PathVariable("version") String version,
            @ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = true) @RequestHeader("aid") String aid,
            @ApiParam(value = "Application namespace", required = true) @RequestHeader(NS) String ns,
            @ApiParam(value = "Authorization", required = true) @RequestHeader(MusicUtil.AUTHORIZATION) String authorization,
            @RequestBody JsonInsert selObj,
            @ApiParam(value = "Keyspace Name", required = true) @PathVariable("keyspace") String keyspace,
            @ApiParam(value = "Table Name", required = true) @PathVariable("tablename") String tablename,
            @RequestParam MultiValueMap<String, String> requestParam) throws Exception {

        if (logger.isDebugEnabled()) {
            logger.debug("Comes inside RestMusicDataAPI createKeySpace controller Version  :: " + version);
            logger.debug("Comes inside RestMusicDataAPI selectCritical controller KeySpace Name :: " + keyspace
                    + "tableName ::" + tablename);
        }

        return musicDataAPIService.selectCritical(VERSION, minorVersion, patchVersion, aid, ns, authorization, selObj,
                keyspace, tablename, requestParam);

    }

    /**
     * This is to get rows
     * 
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     * @throws Exception
     */
    @GetMapping(value = "/{keyspace}/tables/{tablename}/rows", produces = MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Select All or Select Specific", response = Map.class)
    public Response select(@ApiParam(value = "Major Version", required = true) @PathVariable("version") String version,
            @ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = true) @RequestHeader("aid") String aid,
            @ApiParam(value = "Application namespace", required = true) @RequestHeader(NS) String ns,
            @ApiParam(value = "Authorization", required = true) @RequestHeader(MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "Keyspace Name", required = true) @PathVariable("keyspace") String keyspace,
            @ApiParam(value = "Table Name", required = true) @PathVariable("tablename") String tablename,
            @RequestParam MultiValueMap<String, String> requestParam) throws Exception {

        if (logger.isDebugEnabled()) {
            logger.debug("Comes inside RestMusicDataAPI createKeySpace controller Version  :: " + version);
            logger.debug("Comes inside RestMusicDataAPI select controller KeySpace Name :: " + keyspace + "tableName ::"
                    + tablename);
        }

        return musicDataAPIService.select(VERSION, minorVersion, patchVersion, aid, ns, authorization, keyspace,
                tablename, requestParam);
    }

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(RestMusicDataAPI.class);
    private static final String XMINORVERSION = "X-minorVersion";
    private static final String XPATCHVERSION = "X-patchVersion";
    private static final String NS = "ns";
    private static final String VERSION = "v2";
}
