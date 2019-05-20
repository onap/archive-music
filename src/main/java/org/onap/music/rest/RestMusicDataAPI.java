/*
 * ============LICENSE_START==========================================
 *  org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Modifications Copyright (c) 2019 Samsung
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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.onap.music.datastore.Condition;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.CassaIndexObject;
import org.onap.music.datastore.jsonobjects.CassaKeyspaceObject;
import org.onap.music.datastore.jsonobjects.CassaTableObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.response.jsonobjects.JsonResponse;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TableMetadata;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

/* Version 2 Class */
//@Path("/v{version: [0-9]+}/keyspaces")
@Path("/v2/keyspaces")
@Api(value = "Data Api")
public class RestMusicDataAPI {
    /*
     * Header values for Versioning X-minorVersion *** - Used to request or communicate a MINOR
     * version back from the client to the server, and from the server back to the client - This
     * will be the MINOR version requested by the client, or the MINOR version of the last MAJOR
     * version (if not specified by the client on the request) - Contains a single position value
     * (e.g. if the full version is 1.24.5, X-minorVersion = "24") - Is optional for the client on
     * request; however, this header should be provided if the client needs to take advantage of
     * MINOR incremented version functionality - Is mandatory for the server on response
     *
     *** X-patchVersion *** - Used only to communicate a PATCH version in a response for
     * troubleshooting purposes only, and will not be provided by the client on request - This will
     * be the latest PATCH version of the MINOR requested by the client, or the latest PATCH version
     * of the MAJOR (if not specified by the client on the request) - Contains a single position
     * value (e.g. if the full version is 1.24.5, X-patchVersion = "5") - Is mandatory for the
     * server on response  (CURRENTLY NOT USED)
     *
     *** X-latestVersion *** - Used only to communicate an API's latest version - Is mandatory for the
     * server on response, and shall include the entire version of the API (e.g. if the full version
     * is 1.24.5, X-latestVersion = "1.24.5") - Used in the response to inform clients that they are
     * not using the latest version of the API (CURRENTLY NOT USED)
     *
     */

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(RestMusicDataAPI.class);
    private static final String XMINORVERSION = "X-minorVersion";
    private static final String XPATCHVERSION = "X-patchVersion";
    private static final String NS = "ns";
    private static final String VERSION = "v2";

    private class RowIdentifier {
        public String primarKeyValue;
        public StringBuilder rowIdString;
        @SuppressWarnings("unused")
        public PreparedQueryObject queryObject; // the string with all the row
                                                // identifiers separated by AND

        public RowIdentifier(String primaryKeyValue, StringBuilder rowIdString,
                        PreparedQueryObject queryObject) {
            this.primarKeyValue = primaryKeyValue;
            this.rowIdString = rowIdString;
            this.queryObject = queryObject;
        }
    }


    /**
     * Create Keyspace REST
     *
     * @param kspObject
     * @param keyspaceName
     * @return
     * @throws Exception
     */
    @POST
    @Path("/{name}")
    @ApiOperation(value = "Create Keyspace", response = String.class,hidden = true)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createKeySpace(
        @ApiParam(value = "Major Version",required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false) @HeaderParam("aid") String aid,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        @ApiParam(value = "Application namespace",required = true) @HeaderParam(NS) String ns,
        CassaKeyspaceObject kspObject,
        @ApiParam(value = "Keyspace Name",required = true) @PathParam("name") String keyspaceName) {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            EELFLoggerDelegate.mdcPut("keyspace", "( "+keyspaceName+" ) ");
            logger.info(EELFLoggerDelegate.applicationLogger,"In Create Keyspace " + keyspaceName);
            if (MusicUtil.isKeyspaceActive() ) {
                logger.info(EELFLoggerDelegate.applicationLogger,"Creating Keyspace " + keyspaceName);
                
                if(kspObject == null || kspObject.getReplicationInfo() == null) {
                    response.status(Status.BAD_REQUEST);
                    return response.entity(new JsonResponse(ResultType.FAILURE).setError(ResultType.BODYMISSING.getResult()).toMap()).build();
                }
        
                /**
                 * Keyspace creation goes here.
                 */
                
                ResultType result = ResultType.FAILURE;
                try {
                     kspObject.setKeyspaceName(keyspaceName);
                     result = MusicCore.createKeyspace(kspObject, MusicUtil.EVENTUAL);
                     logger.info(EELFLoggerDelegate.applicationLogger, "result = " + result);
                } catch ( MusicServiceException ex) {
                    logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity
                        .WARN, ErrorTypes.MUSICSERVICEERROR, ex);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("err:" + ex.getMessage()).toMap()).build();
                }
        
                return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setMessage("Keyspace " + keyspaceName + " Created").toMap()).build();
            } else {
                String vError = "Keyspace Creation has been turned off. Contact DBA to create the keyspace or set keyspace.active to true.";
                logger.info(EELFLoggerDelegate.applicationLogger,vError);
                logger.error(EELFLoggerDelegate.errorLogger,vError, AppMessages.UNKNOWNERROR,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
                return response.status(Response.Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(vError).toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
        
    }

    /**
     *
     * @param kspObject
     * @param keyspaceName
     * @return
     * @throws Exception
     */
    @DELETE
    @Path("/{name}")
    @ApiOperation(value = "Delete Keyspace", response = String.class,hidden=true)
    @Produces(MediaType.APPLICATION_JSON)
    public Response dropKeySpace(
        @ApiParam(value = "Major Version",required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false) @HeaderParam("aid") String aid,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        @ApiParam(value = "Application namespace",required = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Keyspace Name",required = true) @PathParam("name") String keyspaceName) throws Exception {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            EELFLoggerDelegate.mdcPut("keyspace", "( " + keyspaceName + " ) ");
            logger.info(EELFLoggerDelegate.applicationLogger,"In Drop Keyspace " + keyspaceName);
            if (MusicUtil.isKeyspaceActive()) {
                String consistency = MusicUtil.EVENTUAL;// for now this needs only
                /**
                 * Drop Keyspace goes here
                 */
                CassaKeyspaceObject kspObject = new CassaKeyspaceObject();
                kspObject.setKeyspaceName(keyspaceName);
                ResultType result = MusicCore.dropKeyspace(kspObject, consistency);
                if ( result.equals(ResultType.FAILURE) ) {
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(result).setError("Error Deleteing Keyspace " + keyspaceName).toMap()).build();
                }
                return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setMessage("Keyspace " + keyspaceName + " Deleted").toMap()).build();
            } else {
                String vError = "Keyspace deletion has been turned off. Contact DBA to delete the keyspace or set keyspace.active to true.";
                logger.info(EELFLoggerDelegate.applicationLogger,vError);
                logger.error(EELFLoggerDelegate.errorLogger,vError, AppMessages.UNKNOWNERROR,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
                return response.status(Response.Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(vError).toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     *
     * @param tableObj
     * @param version
     * @param keyspace
     * @param tablename
     * @param headers
     * @return
     * @throws Exception -
     */
    @POST
    @Path("/{keyspace: .*}/tables/{tablename: .*}")
    @ApiOperation(value = "Create Table", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value={
        @ApiResponse(code= 400, message = "Will return JSON response with message"),
        @ApiResponse(code= 401, message = "Unautorized User")
    })
    public Response createTable(
        @ApiParam(value = "Major Version",required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",required = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        CassaTableObject cassaTableObject,
        @ApiParam(value = "Keyspace Name",required = true) @PathParam("keyspace") String keyspace,
        @ApiParam(value = "Table Name",required = true) @PathParam("tablename") String tablename) throws Exception {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            if(keyspace == null || keyspace.isEmpty() || tablename == null || tablename.isEmpty()){
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                        .setError("One or more path parameters are not set, please check and try again."
                            + "Parameter values: keyspace='" + keyspace + "' tablename='" + tablename + "'")
                            .toMap()).build();
            }
            EELFLoggerDelegate.mdcPut("keyspace", "( "+keyspace+" ) ");
                        
            // first read the information about the table fields
            Map<String, String> fields = cassaTableObject.getFields();
            if (fields == null) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("Create Table Error: No fields in request").toMap()).build();
            }

            ResultType result = ResultType.FAILURE;
            try {
                cassaTableObject.setKeyspaceName(keyspace);
                cassaTableObject.setTableName(tablename);
                result = MusicCore.createTable(cassaTableObject, MusicUtil.EVENTUAL);
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.CRITICAL, ErrorTypes.MUSICSERVICEERROR);
                response.status(Status.BAD_REQUEST);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
            if ( result.equals(ResultType.FAILURE) ) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(result).setError("Error Creating Table " + tablename).toMap()).build();
            }
            return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setMessage("TableName " + tablename.trim() + " Created under keyspace " + keyspace.trim()).toMap()).build();
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     *
     * @param keyspace
     * @param tablename
     * @param fieldName
     * @param info
     * @throws Exception
     */
    @POST
    @Path("/{keyspace: .*}/tables/{tablename: .*}/index/{field: .*}")
    @ApiOperation(value = "Create Index", response = String.class)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createIndex(
        @ApiParam(value = "Major Version",required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",required = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        @ApiParam(value = "Keyspace Name",required = true) @PathParam("keyspace") String keyspace,
        @ApiParam(value = "Table Name",required = true) @PathParam("tablename") String tablename,
        @ApiParam(value = "Field Name",required = true) @PathParam("field") String fieldName,
        @Context UriInfo info) throws Exception {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            if ((keyspace == null || keyspace.isEmpty()) || (tablename == null || tablename.isEmpty()) || (fieldName == null || fieldName.isEmpty())){
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("one or more path parameters are not set, please check and try again")
                    .toMap()).build();
            }
            EELFLoggerDelegate.mdcPut("keyspace", "( "+keyspace+" ) ");
            MultivaluedMap<String, String> rowParams = info.getQueryParameters();
            String indexName = "";
            if (rowParams.getFirst("index_name") != null)
                indexName = rowParams.getFirst("index_name");
            
            /**
             * Index Creation will start here.
             */
            CassaIndexObject cassaIndexObject = new CassaIndexObject();
            cassaIndexObject.setIndexName(indexName);
            cassaIndexObject.setKeyspaceName(keyspace);
            cassaIndexObject.setTableName(tablename);
            cassaIndexObject.setFieldName(fieldName);
            
            ResultType result = ResultType.FAILURE;
            try {
                 result = MusicCore.createIndex(cassaIndexObject, MusicUtil.EVENTUAL);
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity
                .CRITICAL, ErrorTypes.GENERALSERVICEERROR, ex);
                response.status(Status.BAD_REQUEST);
                return response.entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
            if ( result.equals(ResultType.SUCCESS) ) {
                return response.status(Status.OK).entity(new JsonResponse(result).setMessage("Index Created on " + keyspace+"."+tablename+"."+fieldName).toMap()).build();
            } else {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(result).setError("Unknown Error in create index.").toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     *
     * @param insObj
     * @param keyspace
     * @param tablename
     * @return
     * @throws Exception
     */
    @POST
    @Path("/{keyspace: .*}/tables/{tablename: .*}/rows")
    @ApiOperation(value = "Insert Into Table", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response insertIntoTable(
        @ApiParam(value = "Major Version",required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",required = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        JsonInsert insObj,
        @ApiParam(value = "Keyspace Name",
            required = true) @PathParam("keyspace") String keyspace,
        @ApiParam(value = "Table Name",
            required = true) @PathParam("tablename") String tablename) {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            if((keyspace == null || keyspace.isEmpty()) || (tablename == null || tablename.isEmpty())){
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("one or more path parameters are not set, please check and try again")
                    .toMap()).build();
            }
            EELFLoggerDelegate.mdcPut("keyspace","(" + keyspace + ")");
            PreparedQueryObject queryObject = new PreparedQueryObject();
            TableMetadata tableInfo = null;
            try {
                tableInfo = MusicDataStoreHandle.returnColumnMetadata(keyspace, tablename);
                if(tableInfo == null) {
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Table name doesn't exists. Please check the table name.").toMap()).build();
                }
            } catch (MusicServiceException e) {
                logger.error(EELFLoggerDelegate.errorLogger, e, AppMessages.UNKNOWNERROR  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
            }
            String primaryKeyName = tableInfo.getPrimaryKey().get(0).getName();
            StringBuilder fieldsString = new StringBuilder("(vector_ts,");
            String vectorTs =
                            String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());
            StringBuilder valueString = new StringBuilder("(" + "?" + ",");
            queryObject.addValue(vectorTs);
            
            Map<String, Object> valuesMap = insObj.getValues();
            if (valuesMap==null) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                        .setError("Nothing to insert. No values provided in request.").toMap()).build();
            }
            int counter = 0;
            String primaryKey = "";
            for (Map.Entry<String, Object> entry : valuesMap.entrySet()) {
                fieldsString.append("" + entry.getKey());
                Object valueObj = entry.getValue();
                if (primaryKeyName.equals(entry.getKey())) {
                    primaryKey = entry.getValue() + "";
                    primaryKey = primaryKey.replace("'", "''");
                }
                DataType colType = null;
                try {
                    colType = tableInfo.getColumn(entry.getKey()).getType();
                } catch(NullPointerException ex) {
                    logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage() +" Invalid column name : "+entry.getKey
                        (), AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR, ex);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Invalid column name : "+entry.getKey()).toMap()).build();
                }

                Object formattedValue = null;
                try {
                    formattedValue = MusicUtil.convertToActualDataType(colType, valueObj);
                } catch (Exception e) {
                    logger.error(EELFLoggerDelegate.errorLogger,e);
                }
                valueString.append("?");

                queryObject.addValue(formattedValue);

                if (counter == valuesMap.size() - 1) {
                    fieldsString.append(")");
                    valueString.append(")");
                } else {
                    fieldsString.append(",");
                    valueString.append(",");
                }
                counter = counter + 1;
            }

            //blobs..
            Map<String, byte[]> objectMap = insObj.getObjectMap();
            if(objectMap != null) {
                for (Map.Entry<String, byte[]> entry : objectMap.entrySet()) {
                    if(counter > 0) {
                        fieldsString.replace(fieldsString.length()-1, fieldsString.length(), ",");
                        valueString.replace(valueString.length()-1, valueString.length(), ",");
                    }
                    fieldsString.append("" + entry.getKey());
                    byte[] valueObj = entry.getValue();
                    if (primaryKeyName.equals(entry.getKey())) {
                        primaryKey = entry.getValue() + "";
                        primaryKey = primaryKey.replace("'", "''");
                    }
                    DataType colType = tableInfo.getColumn(entry.getKey()).getType();
                    ByteBuffer formattedValue = null;
                    if(colType.toString().toLowerCase().contains("blob")) {
                        formattedValue = MusicUtil.convertToActualDataType(colType, valueObj);
                    }
                    valueString.append("?");
                    queryObject.addValue(formattedValue);
                    counter = counter + 1;
                    fieldsString.append(",");
                    valueString.append(",");
                } 
            }

            if(primaryKey == null || primaryKey.length() <= 0) {
                logger.error(EELFLoggerDelegate.errorLogger, "Some required partition key parts are missing: "+primaryKeyName );
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.SYNTAXERROR).setError("Some required partition key parts are missing: "+primaryKeyName).toMap()).build();
            }

            fieldsString.replace(fieldsString.length()-1, fieldsString.length(), ")");
            valueString.replace(valueString.length()-1, valueString.length(), ")");

            queryObject.appendQueryString("INSERT INTO " + keyspace + "." + tablename + " "
                            + fieldsString + " VALUES " + valueString);

            String ttl = insObj.getTtl();
            String timestamp = insObj.getTimestamp();

            if ((ttl != null) && (timestamp != null)) {
                logger.info(EELFLoggerDelegate.applicationLogger, "both there");
                queryObject.appendQueryString(" USING TTL ? AND TIMESTAMP ?");
                queryObject.addValue(Integer.parseInt(ttl));
                queryObject.addValue(Long.parseLong(timestamp));
            }

            if ((ttl != null) && (timestamp == null)) {
                logger.info(EELFLoggerDelegate.applicationLogger, "ONLY TTL there");
                queryObject.appendQueryString(" USING TTL ?");
                queryObject.addValue(Integer.parseInt(ttl));
            }

            if ((ttl == null) && (timestamp != null)) {
                logger.info(EELFLoggerDelegate.applicationLogger, "ONLY timestamp there");
                queryObject.appendQueryString(" USING TIMESTAMP ?");
                queryObject.addValue(Long.parseLong(timestamp));
            }

            queryObject.appendQueryString(";");

            ReturnType result = null;
            String consistency = insObj.getConsistencyInfo().get("type");
            if(consistency.equalsIgnoreCase(MusicUtil.EVENTUAL) && insObj.getConsistencyInfo().get("consistency") != null) {
                if(MusicUtil.isValidConsistency(insObj.getConsistencyInfo().get("consistency"))) {
                    queryObject.setConsistency(insObj.getConsistencyInfo().get("consistency"));
                } else {
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.SYNTAXERROR).setError("Invalid Consistency type").toMap()).build();
                }
            }
            queryObject.setOperation("insert");
            try {
                if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL)) {
                    result = MusicCore.eventualPut(queryObject);
                } else if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                    String lockId = insObj.getConsistencyInfo().get("lockId");
                    if(lockId == null) {
                        logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                                + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
                        return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("LockId cannot be null. Create lock "
                                + "and acquire lock or use ATOMIC instead of CRITICAL").toMap()).build();
                    }
                    result = MusicCore.criticalPut(keyspace, tablename, primaryKey, queryObject, lockId,null);
                } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
                    result = MusicCore.atomicPut(keyspace, tablename, primaryKey, queryObject, null);
                }
            } catch (Exception ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity
                    .WARN, ErrorTypes.MUSICSERVICEERROR, ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
            if (result==null) {
                logger.error(EELFLoggerDelegate.errorLogger,"Null result - Please Contact admin", AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Null result - Please Contact admin").toMap()).build();
            }else if(result.getResult() == ResultType.FAILURE) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(result.getResult()).setError(result.getMessage()).toMap()).build();
            }
            return response.status(Status.OK).entity(new JsonResponse(result.getResult()).setMessage("Insert Successful").toMap()).build();
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     *
     * @param insObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     * @throws MusicServiceException 
     * @throws MusicQueryException 
     * @throws Exception
     */
    @PUT
    @Path("/{keyspace: .*}/tables/{tablename: .*}/rows")
    @ApiOperation(value = "Update Table", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateTable(
        @ApiParam(value = "Major Version",
            required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",
            required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",
            required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",
            required = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        JsonUpdate updateObj,
        @ApiParam(value = "Keyspace Name",
            required = true) @PathParam("keyspace") String keyspace,
        @ApiParam(value = "Table Name",
            required = true) @PathParam("tablename") String tablename,
        @Context UriInfo info) throws MusicQueryException, MusicServiceException {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            if((keyspace == null || keyspace.isEmpty()) || (tablename == null || tablename.isEmpty())){
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("one or more path parameters are not set, please check and try again")
                    .toMap()).build();
            }
            EELFLoggerDelegate.mdcPut("keyspace", "( "+keyspace+" ) ");
            long startTime = System.currentTimeMillis();
            String operationId = UUID.randomUUID().toString();  // just for infoging
                                                                // purposes.
            String consistency = updateObj.getConsistencyInfo().get("type");

            logger.info(EELFLoggerDelegate.applicationLogger, "--------------Music " + consistency
                + " update-" + operationId + "-------------------------");
            // obtain the field value pairs of the update

            PreparedQueryObject queryObject = new PreparedQueryObject();
            Map<String, Object> valuesMap = updateObj.getValues();

            TableMetadata tableInfo;
            try {
                tableInfo = MusicDataStoreHandle.returnColumnMetadata(keyspace, tablename);
            } catch (MusicServiceException e) {
                logger.error(EELFLoggerDelegate.errorLogger,e, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes
                    .GENERALSERVICEERROR, e);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
            }
            if (tableInfo == null) {
                logger.error(EELFLoggerDelegate.errorLogger,"Table information not found. Please check input for table name= "+tablename, AppMessages.MISSINGINFO  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("Table information not found. Please check input for table name= "
                    + keyspace + "." + tablename).toMap()).build();
            }
            String vectorTs = String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());
            StringBuilder fieldValueString = new StringBuilder("vector_ts=?,");
            queryObject.addValue(vectorTs);
            int counter = 0;
            for (Map.Entry<String, Object> entry : valuesMap.entrySet()) {
                Object valueObj = entry.getValue();
                DataType colType = null;
                try {
                    colType = tableInfo.getColumn(entry.getKey()).getType();
                } catch(NullPointerException ex) {
                    logger.error(EELFLoggerDelegate.errorLogger, ex, "Invalid column name : "+entry.getKey(), ex);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Invalid column name : "+entry.getKey()).toMap()).build();
                }
                Object valueString = null;
                try {
                    valueString = MusicUtil.convertToActualDataType(colType, valueObj);
                } catch (Exception e) {
                    logger.error(EELFLoggerDelegate.errorLogger,e);
                }
                fieldValueString.append(entry.getKey() + "= ?");
                queryObject.addValue(valueString);
                if (counter != valuesMap.size() - 1) {
                    fieldValueString.append(",");
                }    
                counter = counter + 1;
            }
            String ttl = updateObj.getTtl();
            String timestamp = updateObj.getTimestamp();

            queryObject.appendQueryString("UPDATE " + keyspace + "." + tablename + " ");
            if ((ttl != null) && (timestamp != null)) {
                logger.info("both there");
                queryObject.appendQueryString(" USING TTL ? AND TIMESTAMP ?");
                queryObject.addValue(Integer.parseInt(ttl));
                queryObject.addValue(Long.parseLong(timestamp));
            }

            if ((ttl != null) && (timestamp == null)) {
                logger.info("ONLY TTL there");
                queryObject.appendQueryString(" USING TTL ?");
                queryObject.addValue(Integer.parseInt(ttl));
            }

            if ((ttl == null) && (timestamp != null)) {
                logger.info("ONLY timestamp there");
                queryObject.appendQueryString(" USING TIMESTAMP ?");
                queryObject.addValue(Long.parseLong(timestamp));
            }
            // get the row specifier
            RowIdentifier rowId = null;
            try {
                rowId = getRowIdentifier(keyspace, tablename, info.getQueryParameters(), queryObject);
                if(rowId == null || rowId.primarKeyValue.isEmpty()) {
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                            .setError("Mandatory WHERE clause is missing. Please check the input request.").toMap()).build();
                }
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes
                    .GENERALSERVICEERROR, ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }

            queryObject.appendQueryString(
                " SET " + fieldValueString + " WHERE " + rowId.rowIdString + ";");

            // get the conditional, if any
            Condition conditionInfo;
            if (updateObj.getConditions() == null) {
                conditionInfo = null;
            } else {
                // to avoid parsing repeatedly, just send the select query to obtain row
                PreparedQueryObject selectQuery = new PreparedQueryObject();
                selectQuery.appendQueryString("SELECT *  FROM " + keyspace + "." + tablename + " WHERE "
                    + rowId.rowIdString + ";");
                selectQuery.addValue(rowId.primarKeyValue);
                conditionInfo = new Condition(updateObj.getConditions(), selectQuery);
            }

            ReturnType operationResult = null;
            long jsonParseCompletionTime = System.currentTimeMillis();

            if(consistency.equalsIgnoreCase(MusicUtil.EVENTUAL) && updateObj.getConsistencyInfo().get("consistency") != null) {
                if(MusicUtil.isValidConsistency(updateObj.getConsistencyInfo().get("consistency"))) {
                    queryObject.setConsistency(updateObj.getConsistencyInfo().get("consistency"));
                } else {
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.SYNTAXERROR).setError("Invalid Consistency type").toMap()).build();
                }
            }
            queryObject.setOperation("update");
            if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL)) {
                operationResult = MusicCore.eventualPut(queryObject);
            } else if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                String lockId = updateObj.getConsistencyInfo().get("lockId");
                if(lockId == null) {
                    logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                            + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("LockId cannot be null. Create lock "
                            + "and acquire lock or use ATOMIC instead of CRITICAL").toMap()).build();
                }
                operationResult = MusicCore.criticalPut(keyspace, tablename, rowId.primarKeyValue,
                                queryObject, lockId, conditionInfo);
            } else if (consistency.equalsIgnoreCase("atomic_delete_lock")) {
                // this function is mainly for the benchmarks
                try {
                    operationResult = MusicCore.atomicPutWithDeleteLock(keyspace, tablename,
                        rowId.primarKeyValue, queryObject, conditionInfo);
                } catch (MusicLockingException e) {
                    logger.error(EELFLoggerDelegate.errorLogger,e, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN,
                        ErrorTypes.GENERALSERVICEERROR, e);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
                }
            } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
                try {
                    operationResult = MusicCore.atomicPut(keyspace, tablename, rowId.primarKeyValue,
                        queryObject, conditionInfo);
                } catch (MusicLockingException e) {
                    logger.error(EELFLoggerDelegate.errorLogger,e, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR, e);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
                }
            } else if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL_NB)) {
                operationResult = MusicCore.eventualPut_nb(queryObject, keyspace, tablename, rowId.primarKeyValue);
            }
            long actualUpdateCompletionTime = System.currentTimeMillis();

            long endTime = System.currentTimeMillis();
            String timingString = "Time taken in ms for Music " + consistency + " update-" + operationId
                + ":" + "|total operation time:" + (endTime - startTime)
                + "|json parsing time:" + (jsonParseCompletionTime - startTime)
                + "|update time:" + (actualUpdateCompletionTime - jsonParseCompletionTime)
                + "|";

            if (operationResult != null && operationResult.getTimingInfo() != null) {
                String lockManagementTime = operationResult.getTimingInfo();
                timingString = timingString + lockManagementTime;
            }
            logger.info(EELFLoggerDelegate.applicationLogger, timingString);

            if (operationResult==null) {
                logger.error(EELFLoggerDelegate.errorLogger,"Null result - Please Contact admin", AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Null result - Please Contact admin").toMap()).build();
            }
            if ( operationResult.getResult() == ResultType.SUCCESS ) {
                return response.status(Status.OK).entity(new JsonResponse(operationResult.getResult()).setMessage(operationResult.getMessage()).toMap()).build();
            } else {
                logger.error(EELFLoggerDelegate.errorLogger,operationResult.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(operationResult.getResult()).setError(operationResult.getMessage()).toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     *
     * @param delObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     * @throws MusicServiceException 
     * @throws MusicQueryException 
     * @throws Exception
     */
    @DELETE
    @Path("/{keyspace: .*}/tables/{tablename: .*}/rows")
    @ApiOperation(value = "Delete From table", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteFromTable(
        @ApiParam(value = "Major Version",
            required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",
            required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",
            required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",
            required = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        JsonDelete delObj,
        @ApiParam(value = "Keyspace Name",
            required = true) @PathParam("keyspace") String keyspace,
        @ApiParam(value = "Table Name",
            required = true) @PathParam("tablename") String tablename,
        @Context UriInfo info) throws MusicQueryException, MusicServiceException {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            if((keyspace == null || keyspace.isEmpty()) || (tablename == null || tablename.isEmpty())){
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("one or more path parameters are not set, please check and try again")
                    .toMap()).build();
            }
            EELFLoggerDelegate.mdcPut("keyspace", "( "+keyspace+" ) ");
            if(delObj == null) {
                logger.error(EELFLoggerDelegate.errorLogger,"Required HTTP Request body is missing.", AppMessages.MISSINGDATA  ,ErrorSeverity.WARN, ErrorTypes.DATAERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Required HTTP Request body is missing.").toMap()).build();
            }
            PreparedQueryObject queryObject = new PreparedQueryObject();
            StringBuilder columnString = new StringBuilder();

            int counter = 0;
            List<String> columnList = delObj.getColumns();
            if (columnList != null) {
                for (String column : columnList) {
                    columnString.append(column);
                    if (counter != columnList.size() - 1)
                        columnString.append(",");
                    counter = counter + 1;
                }
            }

            // get the row specifier
            RowIdentifier rowId = null;
            try {
                rowId = getRowIdentifier(keyspace, tablename, info.getQueryParameters(), queryObject);
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes
                    .GENERALSERVICEERROR, ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
            String rowSpec = rowId.rowIdString.toString();

            if ((columnList != null) && (!rowSpec.isEmpty())) {
                queryObject.appendQueryString("DELETE " + columnString + " FROM " + keyspace + "."
                                + tablename + " WHERE " + rowSpec + ";");
            }

            if ((columnList == null) && (!rowSpec.isEmpty())) {
                queryObject.appendQueryString("DELETE FROM " + keyspace + "." + tablename + " WHERE "
                                + rowSpec + ";");
            }

            if ((columnList != null) && (rowSpec.isEmpty())) {
                queryObject.appendQueryString(
                                "DELETE " + columnString + " FROM " + keyspace + "." + rowSpec + ";");
            }
            // get the conditional, if any
            Condition conditionInfo;
            if (delObj.getConditions() == null) {
                conditionInfo = null;
            } else {
                // to avoid parsing repeatedly, just send the select query to
                // obtain row
                PreparedQueryObject selectQuery = new PreparedQueryObject();
                selectQuery.appendQueryString("SELECT *  FROM " + keyspace + "." + tablename + " WHERE "
                    + rowId.rowIdString + ";");
                selectQuery.addValue(rowId.primarKeyValue);
                conditionInfo = new Condition(delObj.getConditions(), selectQuery);
            }

            String consistency = delObj.getConsistencyInfo().get("type");


            if(consistency.equalsIgnoreCase(MusicUtil.EVENTUAL) && delObj.getConsistencyInfo().get("consistency")!=null) {
                if(MusicUtil.isValidConsistency(delObj.getConsistencyInfo().get("consistency"))) {
                    queryObject.setConsistency(delObj.getConsistencyInfo().get("consistency"));
                } else {
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.SYNTAXERROR)
                        .setError("Invalid Consistency type").toMap()).build();
                }
            }
            ReturnType operationResult = null;
            queryObject.setOperation("delete");
            try {
                if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL))
                    operationResult = MusicCore.eventualPut(queryObject);
                else if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                    String lockId = delObj.getConsistencyInfo().get("lockId");
                    if(lockId == null) {
                        logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                            + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
                        return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("LockId cannot be null. Create lock "
                            + "and acquire lock or use ATOMIC instead of CRITICAL").toMap()).build();
                    }
                    operationResult = MusicCore.criticalPut(keyspace, tablename, rowId.primarKeyValue,
                        queryObject, lockId, conditionInfo);
                } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
                    operationResult = MusicCore.atomicPut(keyspace, tablename, rowId.primarKeyValue,
                        queryObject, conditionInfo);
                } else if(consistency.equalsIgnoreCase(MusicUtil.EVENTUAL_NB)) {                    
                    operationResult = MusicCore.eventualPut_nb(queryObject, keyspace, tablename, rowId.primarKeyValue);
                }
            } catch (MusicLockingException e) {
                logger.error(EELFLoggerDelegate.errorLogger,e, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR, e);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                        .setError("Unable to perform Delete operation. Exception from music").toMap()).build();
            }
            if (operationResult==null) {
                logger.error(EELFLoggerDelegate.errorLogger,"Null result - Please Contact admin", AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Null result - Please Contact admin").toMap()).build();
            }
            if (operationResult.getResult().equals(ResultType.SUCCESS)) {
                return response.status(Status.OK).entity(new JsonResponse(operationResult.getResult()).setMessage(operationResult.getMessage()).toMap()).build();
            } else {
                logger.error(EELFLoggerDelegate.errorLogger,operationResult.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(operationResult.getMessage()).toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     *
     * @param tabObj
     * @param keyspace
     * @param tablename
     * @throws Exception
     */
    @DELETE
    @Path("/{keyspace: .*}/tables/{tablename: .*}")
    @ApiOperation(value = "Drop Table", response = String.class)
    @Produces(MediaType.APPLICATION_JSON)
    public Response dropTable(
        @ApiParam(value = "Major Version",
            required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",
            required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",
            required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",
            required = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        @ApiParam(value = "Keyspace Name",
            required = true) @PathParam("keyspace") String keyspace,
        @ApiParam(value = "Table Name",
            required = true) @PathParam("tablename") String tablename) throws Exception {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            if((keyspace == null || keyspace.isEmpty()) || (tablename == null || tablename.isEmpty())){
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("one or more path parameters are not set, please check and try again")
                    .toMap()).build();
            }
            EELFLoggerDelegate.mdcPut("keyspace", "( "+keyspace+" ) ");
            
            CassaTableObject cassaTableObject = new CassaTableObject();
            cassaTableObject.setKeyspaceName(keyspace);
            cassaTableObject.setTableName(tablename);
            
            try {
                return response.status(Status.OK).entity(new JsonResponse(MusicCore.dropTable(cassaTableObject, MusicUtil.EVENTUAL)).toMap()).build();
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger, ex, AppMessages.MISSINGINFO  ,ErrorSeverity.WARN, ErrorTypes
                    .GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     *
     * @param selObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     */
    @PUT
    @Path("/{keyspace: .*}/tables/{tablename: .*}/rows/criticalget")
    @ApiOperation(value = "Select Critical", response = Map.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response selectCritical(
        @ApiParam(value = "Major Version",
            required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",
            required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",
            required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",
            required = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        JsonInsert selObj,
        @ApiParam(value = "Keyspace Name",
            required = true) @PathParam("keyspace") String keyspace,
        @ApiParam(value = "Table Name",
            required = true) @PathParam("tablename") String tablename,
        @Context UriInfo info) throws Exception {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            if((keyspace == null || keyspace.isEmpty()) || (tablename == null || tablename.isEmpty())){
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("one or more path parameters are not set, please check and try again")
                    .toMap()).build();
            }
            EELFLoggerDelegate.mdcPut("keyspace", "( "+keyspace+" ) ");
            String lockId = selObj.getConsistencyInfo().get("lockId");
            PreparedQueryObject queryObject = new PreparedQueryObject();
            RowIdentifier rowId = null;
            try {
                rowId = getRowIdentifier(keyspace, tablename, info.getQueryParameters(), queryObject);
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes
                    .GENERALSERVICEERROR, ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
            queryObject.appendQueryString(
                "SELECT *  FROM " + keyspace + "." + tablename + " WHERE " + rowId.rowIdString + ";");

            ResultSet results = null;

            String consistency = selObj.getConsistencyInfo().get("type");
            try {
            if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                if(lockId == null) {
                    logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                        + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("LockId cannot be null. Create lock "
                        + "and acquire lock or use ATOMIC instead of CRITICAL").toMap()).build();
                }
                results = MusicCore.criticalGet(keyspace, tablename, rowId.primarKeyValue, queryObject,lockId);
            } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
                results = MusicCore.atomicGet(keyspace, tablename, rowId.primarKeyValue, queryObject);
            }
            }catch(Exception ex) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
            
            if(results!=null && results.getAvailableWithoutFetching() >0) {
                return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setDataResult(MusicDataStoreHandle.marshallResults(results)).toMap()).build();
            }
            return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setError("No data found").toMap()).build();
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     *
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     * @throws Exception
     */
    @GET
    @Path("/{keyspace: .*}/tables/{tablename: .*}/rows")
    @ApiOperation(value = "Select All or Select Specific", response = Map.class)
    @Produces(MediaType.APPLICATION_JSON)
    public Response select(
        @ApiParam(value = "Major Version",
            required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",
            required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",
            required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",
            required = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        @ApiParam(value = "Keyspace Name",
            required = true) @PathParam("keyspace") String keyspace,
        @ApiParam(value = "Table Name",
            required = true) @PathParam("tablename") String tablename,
        @Context UriInfo info) throws Exception {
        try { 
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            if((keyspace == null || keyspace.isEmpty()) || (tablename == null || tablename.isEmpty())){
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("one or more path parameters are not set, please check and try again")
                    .toMap()).build();
            }
            EELFLoggerDelegate.mdcPut("keyspace", "( " + keyspace + " ) ");
            PreparedQueryObject queryObject = new PreparedQueryObject();

            if (info.getQueryParameters().isEmpty()) { // select all
                queryObject.appendQueryString("SELECT *  FROM " + keyspace + "." + tablename + ";");
            } else {
                int limit = -1; // do not limit the number of results
                try {
                    queryObject = selectSpecificQuery(keyspace, tablename, info, limit);
                } catch (MusicServiceException ex) {
                    logger.error(EELFLoggerDelegate.errorLogger, ex, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN,
                        ErrorTypes.GENERALSERVICEERROR, ex);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
                }
            }
            try {
                ResultSet results = MusicCore.get(queryObject);
                if(results.getAvailableWithoutFetching() >0) {
                    return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setDataResult(MusicDataStoreHandle.marshallResults(results)).toMap()).build();
                }
                return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setDataResult(MusicDataStoreHandle.marshallResults(results)).setError("No data found").toMap()).build();
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger, ex, AppMessages.UNKNOWNERROR  ,ErrorSeverity.ERROR,
                    ErrorTypes.MUSICSERVICEERROR, ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     *
     * @param keyspace
     * @param tablename
     * @param info
     * @param limit
     * @return
     * @throws MusicServiceException
     */
    public PreparedQueryObject selectSpecificQuery(String keyspace,
        String tablename, UriInfo info, int limit)
        throws MusicServiceException {
        PreparedQueryObject queryObject = new PreparedQueryObject();
        StringBuilder rowIdString = getRowIdentifier(keyspace, 
            tablename,info.getQueryParameters(),queryObject).rowIdString;
        queryObject.appendQueryString(
            "SELECT *  FROM " + keyspace + "." + tablename + " WHERE " + rowIdString);
        if (limit != -1) {
            queryObject.appendQueryString(" LIMIT " + limit);
        }
        queryObject.appendQueryString(";");
        return queryObject;
    }

    /**
     *
     * @param keyspace
     * @param tablename
     * @param rowParams
     * @param queryObject
     * @return
     * @throws MusicServiceException
     */
    private RowIdentifier getRowIdentifier(String keyspace, String tablename,
        MultivaluedMap<String, String> rowParams, PreparedQueryObject queryObject)
        throws MusicServiceException {
        StringBuilder rowSpec = new StringBuilder();
        int counter = 0;
        TableMetadata tableInfo = MusicDataStoreHandle.returnColumnMetadata(keyspace, tablename);
        if (tableInfo == null) {
            logger.error(EELFLoggerDelegate.errorLogger,
                "Table information not found. Please check input for table name= "
                + keyspace + "." + tablename);
            throw new MusicServiceException(
                "Table information not found. Please check input for table name= "
                + keyspace + "." + tablename);
        }
        StringBuilder primaryKey = new StringBuilder();
        for (MultivaluedMap.Entry<String, List<String>> entry : rowParams.entrySet()) {
            String keyName = entry.getKey();
            List<String> valueList = entry.getValue();
            String indValue = valueList.get(0);
            DataType colType = null;
            Object formattedValue = null;
            try {
                colType = tableInfo.getColumn(entry.getKey()).getType();
                formattedValue = MusicUtil.convertToActualDataType(colType, indValue);
            } catch (Exception e) {
                logger.error(EELFLoggerDelegate.errorLogger,e);
            }
            if(tableInfo.getPrimaryKey().get(0).getName().equals(entry.getKey())) {
                primaryKey.append(indValue);
            }
            rowSpec.append(keyName + "= ?");
            queryObject.addValue(formattedValue);
            if (counter != rowParams.size() - 1) {
                rowSpec.append(" AND ");
            }
            counter = counter + 1;
        }
        return new RowIdentifier(primaryKey.toString(), rowSpec, queryObject);
    }
}
